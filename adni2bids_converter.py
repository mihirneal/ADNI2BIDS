#!/usr/bin/env python3
"""
ADNI4 to BIDS Converter

This script converts ADNI4 DICOM data to BIDS format using dcm2niix directly,
bypassing heudiconv's complexities.

ADNI4 Structure:
dicom/
├── 027_S_6512/          # Subject ID
│   ├── T1/              # Modality directories
│   │   ├── 2022-03-31_13_38_14.0/  # Session timestamp directories
│   │   └── 2023-05-15_09_22_01.0/
│   ├── fMRI/
│   │   ├── 2022-03-31_14_15_30.0/
│   │   └── 2023-05-15_10_45_12.0/
│   └── DTI/
│       └── 2022-03-31_15_20_45.0/

BIDS Output:
BIDS_output/
├── sub-027S6512/        # Subject (underscores removed)
│   ├── ses-20220331/    # Session (YYYYMMDD format)
│   │   ├── anat/        # BIDS modality directories
│   │   ├── func/
│   │   └── dwi/
│   └── ses-20230515/
│       ├── anat/
│       └── func/
"""

import os
import re
import json
import shutil
import subprocess
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('adni2bids_conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ADNI2BIDSConverter:
    """Convert ADNI4 DICOM data to BIDS format using dcm2niix."""
    
    def __init__(self, dicom_root: str, bids_output: str):
        self.dicom_root = Path(dicom_root)
        self.bids_output = Path(bids_output)
        self.bids_output.mkdir(exist_ok=True)
        
        # Create conversion_logs directory
        self.logs_dir = Path("conversion_logs")
        self.logs_dir.mkdir(exist_ok=True)
        
        # Modality mapping from ADNI directory names to BIDS modalities
        self.modality_mapping = {
            # Anatomical - T1w variants
            'MPRAGE': 'anat',
            'MP-RAGE': 'anat', 
            'Accelerated_Sagittal_MPRAGE': 'anat',
            'Accelerated_Sagittal_MPRAGE__MSV21_': 'anat',
            'Accelerated_Sagittal_MPRAGE__MSV22_': 'anat',
            'Accelerated_Sagittal_MPRAGE_ND': 'anat',
            'Sagittal_3D_Accelerated_MPRAGE': 'anat',
            'MPRAGE_GRAPPA2': 'anat',
            'MPRAGE_SENSE2': 'anat',
            'Accelerated_Sag_IR-FSPGR': 'anat',
            'Accelerated_Sagittal_IR-FSPGR': 'anat',
            'Sag_IR-FSPGR': 'anat',
            'Sag_IR-SPGR': 'anat',
            'Accelerated_Sag_IR-SPGR': 'anat',
            'MP-RAGE_REPEAT': 'anat',
            'IR-FSPGR-Repeat': 'anat',
            'REPEAT_SAG_3D_MP_RAGE': 'anat',
            'REPEAT_SAG_3D_MP_RAGE_NO_ANGLE': 'anat',
            'MP_RAGE_SAGITTAL_REPEAT': 'anat',
            'MP_RAGE_SAGITTAL': 'anat',
            'SAG_MPRAGE_NO_ANGLE': 'anat',
            'SAG_MPRAGE_GRAPPA2_NO_ANGLE': 'anat',
            'SAG_3D_MPRAGE': 'anat',
            'SAG_3D_MPRAGE_NO_ANGLE': 'anat',
            'IR-SPGR': 'anat',
            'IR-SPGR_w_acceleration': 'anat',
            'IR-FSPGR': 'anat',
            'IR-FSPGR__replaces_MP-Rage_': 'anat',
            'MP-RAGE-Repeat': 'anat',
            'MPRAGE_Repeat': 'anat',
            'MP-RAGE-REPEAT': 'anat',
            'MPRAGE_repeat': 'anat',
            'CS_Sagittal_MPRAGE__MSV22_': 'anat',
            'Accelerated_Sagittal_MPRAGE_REPEAT': 'anat',
            'Accelerated_Sagittal_MPRAGE_repeat': 'anat',
            'Accelerated_Sagittal_MPRAGE_MSV21': 'anat',
            'Sagittal_3D_Accelerated_MPRAGE__MSV21_': 'anat',
            'Sagittal_3D_Accelerated_MPRAGE_REPEAT': 'anat',
            'Accelerated_Sagittal_MPRAGE_MPR_Cor': 'anat',
            'Accelerated_Sagittal_MPRAGE_MPR_Tra': 'anat',
            'REPEAT_SAG_3D_MPRAGE': 'anat',
            'Accelerated_SAG_IR-SPGR': 'anat',
            'Sag_IR-SPGR-REPEAT': 'anat',
            'HS_Sagittal_MPRAGE__MSV22_': 'anat',
            'MPRAGE_S2_DIS2D': 'anat',
            '3D_T1_SAG': 'anat',
            '3D_MPRAGE': 'anat',
            'VWIP_Coronal_3D_Accelerated_MPRAGE': 'anat',
            
            # Anatomical - T2w/FLAIR variants
            'Sagittal_3D_FLAIR': 'anat',
            'Sagittal_3D_FLAIR__MSV22_': 'anat',
            'Sagittal_3D_FLAIR__MSV23_': 'anat',
            'Axial_FLAIR': 'anat',
            'Sagittal_3D_T2_SPACE__MSV21_': 'anat',
            'Sagittal_3D_T2_Vista__MSV21_': 'anat',
            'CS_Sagittal_3D_T2_Vista__MSV24_': 'anat',
            'Sagittal_3D_T2_SPACE_MSV21': 'anat',
            'AXIAL_FLAIR': 'anat',
            'FLAIR': 'anat',
            't2_flair_SAG': 'anat',
            'Sagittal_3D_FLAIR_MSV33': 'anat',
            'Sagittal_3D_FLAIR_MPR_Cor': 'anat',
            'Sagittal_3D_FLAIR_MPR_Tra': 'anat',
            'CS_Sagittal_3D_FLAIR__MSV24_': 'anat',
            'Sagittal_3D_FLAIR__MSV23__RPT': 'anat',
            'Sagittal_3D_FLAIR_Repeat': 'anat',
            'Axial_3D_FLAIR': 'anat',
            
            # Functional
            'Axial_rsfMRI__Eyes_Open_': 'func',
            'Axial_rsfMRI__EYES_OPEN_': 'func',
            'Axial_fcMRI__EYES_OPEN_': 'func',
            'Axial_fcMRI__Eyes_Open_': 'func',
            'Axial_MB_rsfMRI__Eyes_Open_': 'func',
            'Axial_HB_rsfMRI__Eyes_Open___MSV22_': 'func',
            'Axial_HB_rsfMRI__Eyes_Open_': 'func',
            'Resting_State_fMRI': 'func',
            'Extended_Resting_State_fMRI': 'func',
            'Axial_fcMRI': 'func',
            'Axial_MB_rsfMRI__EYES_OPEN___MSV22_': 'func',
            'Axial_rsfMRI__Eyes_Open__MSV21_': 'func',
            'Axial_rsfMRI__Eyes_Open___MSV21': 'func',
            'Axial_rsfMRI__Eyes_Open___MSV21_': 'func',
            'Axial_fcMRI__EYES_OPEN__REPEAT': 'func',
            'AXIAL_RS_fMRI__EYES_OPEN_': 'func',
            'Axial_MB_rsfMRI_AP': 'func',
            'Extended_AXIAL_rsfMRI_EYES_OPEN': 'func',
            'Axial_RESTING_fcMRI__EYES_OPEN_': 'func',
            'Axial_-_Advanced_fMRI_64_Channel': 'func',
            'epi_2s_resting_state': 'func',
            
            # Diffusion
            'Axial_MB_DTI_PA__MSV21_': 'dwi',
            'Axial_MB_DTI_AP__MSV21_': 'dwi',
            'Axial_HB_dMRI__MS21_': 'dwi',
            'Axial_MB_dMRI_PA__MSV21_': 'dwi',
            'Axial_MB_dMRI_AP__MSV21_': 'dwi',
            'Axial_DTI': 'dwi',
            'Axial_DTI__MSV21_': 'dwi',
            'Axial_MB_dMRI_A__P__MSV21_': 'dwi',
            'Axial_MB_dMRI_P__A__MSV21_': 'dwi',
            'Axial_dMRI__MSV21_': 'dwi',
            'Axial_MB_DTI': 'dwi',
            'Axial_DTI__MSV20_': 'dwi',
            'Axial_DTI_MSV21': 'dwi',
            
            # Fieldmaps
            'Axial_Field_Mapping': 'fmap',
            'Field_Mapping': 'fmap',
            'WIP_Field_Mapping': 'fmap',
            'Field_Mapping_REPEAT': 'fmap',
            'Field_Mapping_repeat': 'fmap',
            
            # Perfusion
            'Perfusion_Weighted': 'perf',
            'ASL_Perfusion': 'perf',
            'Axial_2D_PASL': 'perf',
            'Axial_3D_PASL': 'perf',
            'SOURCE_-_Axial_2D_PASL': 'perf',
            'Axial_3D_PASL__Eyes_Open_': 'perf',
            'WIP_SOURCE_-_Axial_3D_pCASL__Eyes_Open_': 'perf',
            
            # Exclude these (scouts, calibration, derived data)
            'AAHead_Scout': 'exclude',
            'AAHead_Scout_MPR_sag': 'exclude',
            'AAHead_Scout_MPR_cor': 'exclude',
            'AAHead_Scout_MPR_tra': 'exclude',
            'Calibration_Scan': 'exclude',
            'relCBF': 'exclude',  # Derived perfusion data
            'MoCoSeries': 'exclude',  # Motion corrected series
            'Cal_8HRBRAIN': 'exclude',
            'B1-Calibration_PA': 'exclude',
            'B1-Calibration_Body': 'exclude',
            'B1-calibration_Body': 'exclude',
            'B1-calibration_Head': 'exclude',
            'SAG_B1_CALIBRATION_BODY': 'exclude',
            'SAG_B1_CALIBRATION_HEAD': 'exclude',
            'SAG_B1_CALIBRATION_BODY_REPEAT': 'exclude',
            'repeat_SAG_B1_CALIBRATION_BODY': 'exclude',
            'Cal_Head_24': 'exclude',
            'ASSET_Cal': 'exclude',
            'Axial_MB_DTI_TENSOR_B0': 'exclude',  # Derived DTI data
            'Axial_MB_DTI_FA': 'exclude',  # Derived DTI data
            'Axial_MB_DTI_ADC': 'exclude',  # Derived DTI data
            'Axial_MB_DTI_TRACEW': 'exclude',  # Derived DTI data
            'Axial_T2_Star-Repeated_with_exact_copy_of_FLAIR': 'exclude',
            'CORONAL': 'exclude',  # Likely localizer/scout
            'Cal_RM_8HRBRAIN': 'exclude',  # Calibration scan variant
            'AXIAL_RFORMAT_1': 'exclude',  # Reformatted/derived data
            'AAHead_Scout_64ch-head-coil': 'exclude',  # Scout with 64-channel coil
            'AAHead_Scout_64ch-head-coil_MPR_sag': 'exclude',  # Scout MPR sagittal
            'B1-Calibration': 'exclude',  # B1 calibration scan
            'Cal_Head+Neck_40': 'exclude',  # Calibration scan for head+neck 40ch
            'act_te_=_6000_B1-Calibration_Body': 'exclude',  # Parametric B1 calibration
            'act_te_=_6000_B1-Calibration_PA': 'exclude',  # Parametric B1 calibration PA
            'Localizer': 'exclude',  # Localizer/scout scan
            'Localizer_MPR_sag': 'exclude'  # Localizer MPR sagittal
        }
        
        # BIDS suffix mapping for specific modalities
        self.suffix_mapping = {
            'anat': {
                # T1w variants
                'MPRAGE': 'T1w',
                'MP-RAGE': 'T1w',
                'Accelerated_Sagittal_MPRAGE': 'T1w',
                'Accelerated_Sagittal_MPRAGE__MSV21_': 'T1w',
                'Accelerated_Sagittal_MPRAGE__MSV22_': 'T1w',
                'Accelerated_Sagittal_MPRAGE_ND': 'T1w',
                'Sagittal_3D_Accelerated_MPRAGE': 'T1w',
                'MPRAGE_GRAPPA2': 'T1w',
                'MPRAGE_SENSE2': 'T1w',
                'Accelerated_Sag_IR-FSPGR': 'T1w',
                'Accelerated_Sagittal_IR-FSPGR': 'T1w',
                'Sag_IR-FSPGR': 'T1w',
                'Sag_IR-SPGR': 'T1w',
                'Accelerated_Sag_IR-SPGR': 'T1w',
                'MP-RAGE_REPEAT': 'T1w',
                'IR-FSPGR-Repeat': 'T1w',
                'IR-FSPGR': 'T1w',
                'IR-SPGR': 'T1w',
                # T2w/FLAIR variants
                'Sagittal_3D_FLAIR': 'FLAIR',
                'Sagittal_3D_FLAIR__MSV22_': 'FLAIR',
                'Sagittal_3D_FLAIR__MSV23_': 'FLAIR',
                'Axial_FLAIR': 'FLAIR',
                'AXIAL_FLAIR': 'FLAIR',
                'FLAIR': 'FLAIR',
                't2_flair_SAG': 'FLAIR',
                'Axial_3D_FLAIR': 'FLAIR',
                'Sagittal_3D_T2_SPACE__MSV21_': 'T2w',
                'Sagittal_3D_T2_Vista__MSV21_': 'T2w',
                'CS_Sagittal_3D_T2_Vista__MSV24_': 'T2w',
                'Sagittal_3D_T2_SPACE_MSV21': 'T2w',
                'default': 'T1w'
            },
            'func': {
                'default': 'task-rest_bold'
            },
            'dwi': {
                # Add phase encoding direction to DWI
                'Axial_MB_DTI_PA__MSV21_': 'dir-PA_dwi',
                'Axial_MB_DTI_AP__MSV21_': 'dir-AP_dwi',
                'Axial_MB_dMRI_PA__MSV21_': 'dir-PA_dwi',
                'Axial_MB_dMRI_AP__MSV21_': 'dir-AP_dwi',
                'Axial_MB_dMRI_A__P__MSV21_': 'dir-AP_dwi',
                'Axial_MB_dMRI_P__A__MSV21_': 'dir-PA_dwi',
                'default': 'dwi'
            },
            'fmap': {
                'default': 'fieldmap'
            },
            'perf': {
                'default': 'asl'
            }
        }
    
    def discover_subjects(self) -> List[str]:
        """Discover all subject directories in the DICOM root."""
        subjects = []
        if not self.dicom_root.exists():
            logger.error(f"DICOM root directory does not exist: {self.dicom_root}")
            return subjects
            
        for item in self.dicom_root.iterdir():
            if item.is_dir() and re.match(r'\d{3}_S_\d{4}', item.name):
                subjects.append(item.name)
                logger.info(f"Found subject: {item.name}")
        
        logger.info(f"Discovered {len(subjects)} subjects")
        return sorted(subjects)
    
    def extract_sessions_for_subject(self, subject_id: str) -> Dict[str, List[Tuple[str, str]]]:
        """
        Extract all sessions for a subject by scanning modality directories.
        
        Returns:
            Dict mapping session_date -> list of (modality_dir, session_timestamp_dir) tuples
        """
        subject_path = self.dicom_root / subject_id
        sessions = defaultdict(list)
        
        if not subject_path.exists():
            logger.warning(f"Subject path does not exist: {subject_path}")
            return dict(sessions)
        
        logger.info(f"Scanning sessions for subject {subject_id}")
        
        # Scan all modality directories
        for modality_dir in subject_path.iterdir():
            if not modality_dir.is_dir():
                continue
                
            logger.debug(f"  Scanning modality: {modality_dir.name}")
            
            # Look for timestamp directories within each modality
            for session_dir in modality_dir.iterdir():
                if not session_dir.is_dir():
                    continue
                
                # Check if this looks like a timestamp directory (YYYY-MM-DD_HH_MM_SS.S)
                if re.match(r'\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}\.\d+', session_dir.name):
                    # Extract date: 2022-03-31_13_38_14.0 -> 20220331
                    session_date = session_dir.name.split('_')[0].replace('-', '')
                    sessions[session_date].append((modality_dir.name, session_dir.name))
                    logger.debug(f"    Found session {session_date} in {modality_dir.name}/{session_dir.name}")
        
        logger.info(f"Subject {subject_id} has {len(sessions)} unique sessions: {list(sessions.keys())}")
        return dict(sessions)
    
    def map_modality_to_bids(self, modality_dir: str) -> Tuple[str, str]:
        """
        Map ADNI modality directory name to BIDS modality and suffix.
        
        Returns:
            (bids_modality, bids_suffix) or (None, None) if excluded
        """
        # Check for exact match first
        if modality_dir in self.modality_mapping:
            bids_modality = self.modality_mapping[modality_dir]
            if bids_modality == 'exclude':
                return None, None
        else:
            # Try partial matching for unknown variants
            modality_upper = modality_dir.upper()
            bids_modality = None
            for adni_name, bids_name in self.modality_mapping.items():
                if bids_name == 'exclude':
                    continue
                if adni_name.upper() in modality_upper or modality_upper in adni_name.upper():
                    bids_modality = bids_name
                    break
            
            if not bids_modality:
                logger.warning(f"Unknown modality directory: {modality_dir}, defaulting to 'other'")
                return 'other', 'unknown'
        
        # Determine BIDS suffix
        if bids_modality in self.suffix_mapping:
            suffix_map = self.suffix_mapping[bids_modality]
            # Check for exact match first
            if modality_dir in suffix_map:
                return bids_modality, suffix_map[modality_dir]
            # Use default suffix
            if 'default' in suffix_map:
                return bids_modality, suffix_map['default']
        
        logger.warning(f"No suffix mapping for {modality_dir} in {bids_modality}")
        return bids_modality, 'unknown'
    
    def convert_session_with_dcm2niix(self, subject_id: str, session_date: str, 
                                    session_data: List[Tuple[str, str]], subject_logger=None) -> bool:
        """
        Convert all modalities for a single session using dcm2niix.
        
        Args:
            subject_id: ADNI subject ID (e.g., '027_S_6512')
            session_date: Session date in YYYYMMDD format
            session_data: List of (modality_dir, session_timestamp_dir) tuples
        """
        # Convert subject ID to BIDS format (remove underscores)
        bids_subject = subject_id.replace('_', '')
        session_id = f"ses-{session_date}"
        
        # Create BIDS subject/session directory
        bids_subject_dir = self.bids_output / f"sub-{bids_subject}"
        bids_session_dir = bids_subject_dir / session_id
        
        logger.info(f"Converting {subject_id} {session_id}")
        
        success = True
        conversion_count = 0
        
        for modality_dir, session_timestamp_dir in session_data:
            try:
                # Map to BIDS modality and suffix
                bids_modality, bids_suffix = self.map_modality_to_bids(modality_dir)
                
                # Skip excluded modalities
                if bids_modality is None:
                    logger.info(f"  Skipping excluded modality: {modality_dir}")
                    if subject_logger:
                        subject_logger.info(f"    Skipped (excluded): {modality_dir}")
                    continue
                
                # Create BIDS modality directory
                bids_modality_dir = bids_session_dir / bids_modality
                bids_modality_dir.mkdir(parents=True, exist_ok=True)
                
                # Source DICOM directory
                dicom_source = self.dicom_root / subject_id / modality_dir / session_timestamp_dir
                
                if not dicom_source.exists():
                    logger.warning(f"DICOM source does not exist: {dicom_source}")
                    continue
                
                # Count DICOM files
                dicom_files = list(dicom_source.glob("**/*.dcm")) + list(dicom_source.glob("**/*.DCM"))
                if not dicom_files:
                    logger.warning(f"No DICOM files found in {dicom_source}")
                    continue
                
                logger.info(f"  Converting {len(dicom_files)} DICOMs from {modality_dir}/{session_timestamp_dir}")
                
                # Run dcm2niix
                output_filename = f"sub-{bids_subject}_{session_id}_{bids_suffix}"
                
                cmd = [
                    'dcm2niix',
                    '-z', 'y',           # Compress output
                    '-b', 'y',           # Create BIDS sidecar JSON
                    '-ba', 'n',          # Don't anonymize BIDS
                    '-f', output_filename,  # Output filename
                    '-o', str(bids_modality_dir),  # Output directory
                    str(dicom_source)    # Input directory
                ]
                
                logger.debug(f"Running: {' '.join(cmd)}")
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info(f"  ✅ Successfully converted {modality_dir} -> {bids_modality}/{bids_suffix}")
                    if subject_logger:
                        subject_logger.info(f"    ✅ {modality_dir} -> {bids_modality}/{bids_suffix} ({len(dicom_files)} DICOMs)")
                    conversion_count += 1
                else:
                    logger.error(f"  ❌ dcm2niix failed for {modality_dir}")
                    logger.error(f"     stdout: {result.stdout}")
                    logger.error(f"     stderr: {result.stderr}")
                    if subject_logger:
                        subject_logger.error(f"    ❌ FAILED: {modality_dir} -> {bids_modality}/{bids_suffix}")
                        subject_logger.error(f"       Error: {result.stderr}")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error converting {modality_dir}/{session_timestamp_dir}: {e}")
                success = False
        
        logger.info(f"Session {session_id} conversion complete: {conversion_count} modalities converted")
        return success
    
    def generate_modality_index(self, subjects: List[str] = None) -> Dict[str, int]:
        """
        Generate an index of all modality directory names found in the dataset.
        This helps identify unknown modalities that need mapping.
        """
        if subjects is None:
            subjects = self.discover_subjects()
        
        modality_counts = defaultdict(int)
        
        for subject_id in subjects:
            subject_path = self.dicom_root / subject_id
            if not subject_path.exists():
                continue
                
            for modality_dir in subject_path.iterdir():
                if modality_dir.is_dir():
                    modality_counts[modality_dir.name] += 1
        
        logger.info(f"Found {len(modality_counts)} unique modality directories:")
        for modality, count in sorted(modality_counts.items(), key=lambda x: x[1], reverse=True):
            mapped_modality, _ = self.map_modality_to_bids(modality)
            status = "✅" if mapped_modality != 'other' else "❓"
            logger.info(f"  {status} {modality}: {count} subjects -> {mapped_modality}")
        
        return dict(modality_counts)
    
    def convert_subject(self, subject_id: str) -> bool:
        """Convert a single subject's data to BIDS format."""
        logger.info(f"Starting conversion for subject {subject_id}")
        
        # Create per-subject log file
        subject_log_file = self.logs_dir / f"{subject_id}_conversion.log"
        subject_logger = logging.getLogger(f"subject_{subject_id}")
        subject_logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in subject_logger.handlers[:]:
            subject_logger.removeHandler(handler)
            
        subject_handler = logging.FileHandler(subject_log_file)
        subject_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        subject_logger.addHandler(subject_handler)
        
        subject_logger.info(f"=== ADNI2BIDS Conversion Report for Subject {subject_id} ===")
        
        # Extract all sessions for this subject
        sessions = self.extract_sessions_for_subject(subject_id)
        
        if not sessions:
            logger.warning(f"No sessions found for subject {subject_id}")
            subject_logger.error(f"No sessions found for subject {subject_id}")
            return False
        
        subject_logger.info(f"Subject ID: {subject_id}")
        subject_logger.info(f"BIDS Subject: sub-{subject_id.replace('_', '')}")
        subject_logger.info(f"Total Sessions Found: {len(sessions)}")
        subject_logger.info(f"Session Dates: {sorted(sessions.keys())}")
        subject_logger.info("")
        
        success = True
        converted_sessions = {}
        
        for session_date, session_data in sorted(sessions.items()):
            subject_logger.info(f"--- Converting Session: ses-{session_date} ---")
            session_success = self.convert_session_with_dcm2niix(subject_id, session_date, session_data, subject_logger)
            success = success and session_success
            
            # Track what was converted for this session
            if session_success:
                converted_sessions[session_date] = self._get_converted_modalities(subject_id, session_date)
                subject_logger.info(f"Session ses-{session_date} converted successfully")
                subject_logger.info(f"Modalities: {list(converted_sessions[session_date].keys())}")
            else:
                subject_logger.error(f"Session ses-{session_date} conversion FAILED")
            subject_logger.info("")
        
        # Summary
        subject_logger.info("=== CONVERSION SUMMARY ===")
        subject_logger.info(f"Subject: {subject_id}")
        subject_logger.info(f"Total Sessions: {len(sessions)}")
        subject_logger.info(f"Successfully Converted Sessions: {len(converted_sessions)}")
        
        for session_date, modalities in converted_sessions.items():
            subject_logger.info(f"  ses-{session_date}:")
            for modality, files in modalities.items():
                subject_logger.info(f"    {modality}: {len(files)} files")
        
        if not success:
            subject_logger.error("❌ CONVERSION FAILED")
        else:
            subject_logger.info("✅ CONVERSION SUCCESSFUL")
        
        # Close handler
        subject_handler.close()
        subject_logger.removeHandler(subject_handler)
        
        return success
    
    def _get_converted_modalities(self, subject_id: str, session_date: str) -> Dict[str, List[str]]:
        """Get list of converted modalities and files for a session."""
        bids_subject = subject_id.replace('_', '')
        session_dir = self.bids_output / f"sub-{bids_subject}" / f"ses-{session_date}"
        
        modalities = {}
        if session_dir.exists():
            for modality_dir in session_dir.iterdir():
                if modality_dir.is_dir():
                    files = list(modality_dir.glob("*.nii.gz"))
                    if files:
                        modalities[modality_dir.name] = [f.name for f in files]
        
        return modalities
    
    def convert_all_subjects(self, subjects: List[str] = None) -> Dict[str, bool]:
        """Convert all subjects to BIDS format."""
        if subjects is None:
            subjects = self.discover_subjects()
        
        logger.info(f"Starting conversion of {len(subjects)} subjects")
        
        # Generate modality index first
        self.generate_modality_index(subjects)
        
        results = {}
        for subject_id in subjects:
            try:
                results[subject_id] = self.convert_subject(subject_id)
            except Exception as e:
                logger.error(f"Failed to convert subject {subject_id}: {e}")
                results[subject_id] = False
        
        # Summary
        successful = sum(results.values())
        total = len(results)
        logger.info(f"Conversion complete: {successful}/{total} subjects successful")
        
        if successful < total:
            failed_subjects = [s for s, success in results.items() if not success]
            logger.warning(f"Failed subjects: {failed_subjects}")
        
        return results


def main():
    """Main conversion script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert ADNI4 DICOM data to BIDS format')
    parser.add_argument('dicom_root', help='Root directory containing ADNI subject directories')
    parser.add_argument('bids_output', help='Output directory for BIDS dataset')
    parser.add_argument('--subject', help='Convert only this subject (e.g., 027_S_6512)')
    parser.add_argument('--index-only', action='store_true', help='Only generate modality index, don\'t convert')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize converter
    converter = ADNI2BIDSConverter(args.dicom_root, args.bids_output)
    
    if args.index_only:
        converter.generate_modality_index()
        return
    
    if args.subject:
        # Convert single subject
        success = converter.convert_subject(args.subject)
        exit(0 if success else 1)
    else:
        # Convert all subjects
        results = converter.convert_all_subjects()
        failed_count = sum(1 for success in results.values() if not success)
        exit(0 if failed_count == 0 else 1)


if __name__ == '__main__':
    main()