#!/usr/bin/env python3

import pydicom
import os
import glob

def extract_dicom_metadata(dicom_path, description):
    """Extract Protocol Name and Series Description from a DICOM file"""
    try:
        ds = pydicom.dcmread(dicom_path)
        
        print(f"\n=== {description} ===")
        print(f"File: {dicom_path}")
        
        # Extract Protocol Name
        protocol_name = getattr(ds, 'ProtocolName', 'Not found')
        print(f"ProtocolName: {protocol_name}")
        
        # Extract Series Description
        series_description = getattr(ds, 'SeriesDescription', 'Not found')
        print(f"SeriesDescription: {series_description}")
        
        # Additional useful metadata
        sequence_name = getattr(ds, 'SequenceName', 'Not found')
        print(f"SequenceName: {sequence_name}")
        
        pulse_sequence_name = getattr(ds, 'PulseSequenceName', 'Not found')
        print(f"PulseSequenceName: {pulse_sequence_name}")
        
        return {
            'protocol_name': protocol_name,
            'series_description': series_description,
            'sequence_name': sequence_name,
            'pulse_sequence_name': pulse_sequence_name
        }
        
    except Exception as e:
        print(f"Error reading {dicom_path}: {e}")
        return None

def find_and_extract_dicom_metadata():
    """Find DICOM files and extract metadata"""
    
    # For fMRI
    fmri_pattern = "dicom/002_S_0295/Resting_State_fMRI/**/*.dcm"
    fmri_files = glob.glob(fmri_pattern, recursive=True)
    if fmri_files:
        extract_dicom_metadata(fmri_files[0], "fMRI - 002_S_0295")
    
    # For Fieldmaps
    fieldmap_pattern = "dicom/002_S_0295/Field_Mapping/**/*.dcm"
    fieldmap_files = glob.glob(fieldmap_pattern, recursive=True)
    if fieldmap_files:
        extract_dicom_metadata(fieldmap_files[0], "Field Mapping - 002_S_0295")
    
    # For working subject (037_S_4432)
    perfusion_pattern = "dicom/037_S_4432/Perfusion_Weighted/**/*.dcm"
    perfusion_files = glob.glob(perfusion_pattern, recursive=True)
    if perfusion_files:
        extract_dicom_metadata(perfusion_files[0], "Perfusion Weighted - 037_S_4432")
    
    # Let's also check some other common scan types from 002_S_0295
    mprage_pattern = "dicom/002_S_0295/MPRAGE/**/*.dcm"
    mprage_files = glob.glob(mprage_pattern, recursive=True)
    if mprage_files:
        extract_dicom_metadata(mprage_files[0], "MPRAGE - 002_S_0295")

if __name__ == "__main__":
    find_and_extract_dicom_metadata()