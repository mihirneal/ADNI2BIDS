#!/usr/bin/env python3
"""
Script to count modalities in ADNI BIDS dataset.
Provides overall modality counts and detailed anatomical modality breakdown.
"""

import os
from pathlib import Path
from collections import defaultdict, Counter
import re

def count_modalities(bids_root):
    """Count modalities across the entire BIDS dataset."""
    bids_path = Path(bids_root)
    
    if not bids_path.exists():
        print(f"Error: BIDS directory {bids_root} does not exist")
        return
    
    modality_counts = Counter()
    anat_specific_counts = Counter()
    
    # Walk through all subject directories
    for subject_dir in bids_path.glob("sub-*"):
        if not subject_dir.is_dir():
            continue
            
        # Walk through all session directories (if any)
        session_dirs = list(subject_dir.glob("ses-*"))
        if session_dirs:
            # Dataset has sessions
            for session_dir in session_dirs:
                if session_dir.is_dir():
                    count_modalities_in_directory(session_dir, modality_counts, anat_specific_counts)
        else:
            # No sessions, check modality folders directly under subject
            count_modalities_in_directory(subject_dir, modality_counts, anat_specific_counts)
    
    # Print results
    print("="*60)
    print("ADNI BIDS Dataset Modality Counts")
    print("="*60)
    print(f"Dataset path: {bids_root}")
    print()
    
    print("Overall Modality Counts:")
    print("-" * 30)
    for modality in sorted(modality_counts.keys()):
        print(f"{modality:15}: {modality_counts[modality]:6}")
    print(f"{'TOTAL':15}: {sum(modality_counts.values()):6}")
    
    if anat_specific_counts:
        print()
        print("Anatomical Modality Breakdown:")
        print("-" * 35)
        for anat_type in sorted(anat_specific_counts.keys()):
            print(f"{anat_type:20}: {anat_specific_counts[anat_type]:6}")
        print(f"{'TOTAL anat':20}: {sum(anat_specific_counts.values()):6}")

def count_modalities_in_directory(directory, modality_counts, anat_specific_counts):
    """Count modalities in a specific directory (subject or session)."""
    
    # Check each modality folder
    for modality_dir in directory.iterdir():
        if not modality_dir.is_dir():
            continue
            
        modality_name = modality_dir.name
        
        # Count files in this modality directory
        file_count = 0
        for file_path in modality_dir.rglob("*.nii.gz"):
            file_count += 1
            modality_counts[modality_name] += 1
            
            # For anatomical data, get more specific breakdown
            if modality_name == "anat":
                filename = file_path.name
                # Extract modality from filename (e.g., T1w, T2w, FLAIR, etc.)
                match = re.search(r'_([A-Za-z0-9]+)\.nii\.gz$', filename)
                if match:
                    anat_type = match.group(1)
                    anat_specific_counts[anat_type] += 1

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Count modalities in ADNI BIDS dataset")
    parser.add_argument("bids_root", help="Path to BIDS dataset root directory")
    
    args = parser.parse_args()
    count_modalities(args.bids_root)