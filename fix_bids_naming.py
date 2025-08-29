#!/usr/bin/env python3
"""
Script to fix ADNI BIDS dataset naming conventions by converting
letter suffixes (a, b, c) to proper BIDS numerical suffixes (_01, _02, _03)
"""

import json
import os
import shutil
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

def load_issues_data(json_file):
    """Load the issues data from the analysis script"""
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data['issues'], data['stats']

def plan_renames(issues):
    """
    Plan all the renames needed, grouping files by base name and assigning
    proper numerical suffixes in alphabetical order
    """
    rename_plan = []
    
    for subject_id, subject_data in issues.items():
        for session_id, session_data in subject_data.items():
            for modality, files in session_data.items():
                # Group files by base name (e.g., T1w, dwi, etc.)
                by_base_name = defaultdict(list)
                for file_info in files:
                    by_base_name[file_info['base_name']].append(file_info)
                
                # For each base name, sort by suffix and assign numbers
                for base_name, file_list in by_base_name.items():
                    # Sort by suffix (a, b, c, etc.)
                    sorted_files = sorted(file_list, key=lambda x: x['suffix'])
                    
                    # Check if base file (without suffix) exists in the same directory
                    sample_path = Path(sorted_files[0]['full_path'])
                    directory = sample_path.parent
                    subject_session = sample_path.name.split('_T1w')[0] if 'T1w' in sample_path.name else sample_path.name.split(f'_{base_name}')[0]
                    base_filename = f"{subject_session}_{base_name}.nii.gz"
                    base_file_path = directory / base_filename
                    base_file_exists = base_file_path.exists()
                    
                    # If base file exists, add it to rename plan as _01
                    if base_file_exists:
                        base_new_filename = f"{subject_session}_{base_name}_01.nii.gz"
                        base_new_path = str(directory / base_new_filename)
                        
                        rename_plan.append({
                            'old_path': str(base_file_path),
                            'new_path': base_new_path,
                            'old_filename': base_filename,
                            'new_filename': base_new_filename,
                            'subject': subject_id,
                            'session': session_id,
                            'modality': modality,
                            'base_name': base_name,
                            'old_suffix': '',
                            'new_suffix': '01'
                        })
                        
                        # Add corresponding JSON file if it exists
                        base_json_path = str(base_file_path).replace('.nii.gz', '.json')
                        if os.path.exists(base_json_path):
                            base_json_new_path = base_new_path.replace('.nii.gz', '.json')
                            rename_plan.append({
                                'old_path': base_json_path,
                                'new_path': base_json_new_path,
                                'old_filename': base_filename.replace('.nii.gz', '.json'),
                                'new_filename': base_new_filename.replace('.nii.gz', '.json'),
                                'subject': subject_id,
                                'session': session_id,
                                'modality': modality,
                                'base_name': base_name,
                                'old_suffix': '',
                                'new_suffix': '01'
                            })
                    
                    # Start numbering suffixed files from 02 if base exists, otherwise from 01
                    start_num = 2 if base_file_exists else 1
                    
                    for i, file_info in enumerate(sorted_files, start_num):
                        old_path = file_info['full_path']
                        old_filename = file_info['filename']
                        
                        # Create new filename with proper numbering
                        # Replace the suffix (e.g., 'a') with proper number (e.g., '_01')
                        base_part = old_filename.replace(f"{base_name}{file_info['suffix']}", f"{base_name}")
                        new_filename = base_part.replace(f"_{base_name}.nii.gz", f"_{base_name}_{i:02d}.nii.gz")
                        
                        new_path = str(Path(old_path).parent / new_filename)
                        
                        # Also plan rename for corresponding JSON file if it exists
                        json_old_path = old_path.replace('.nii.gz', '.json')
                        json_new_path = new_path.replace('.nii.gz', '.json')
                        
                        rename_plan.append({
                            'old_path': old_path,
                            'new_path': new_path,
                            'old_filename': old_filename,
                            'new_filename': new_filename,
                            'subject': subject_id,
                            'session': session_id,
                            'modality': modality,
                            'base_name': base_name,
                            'old_suffix': file_info['suffix'],
                            'new_suffix': f"{i:02d}"
                        })
                        
                        # Add JSON file if it exists
                        if os.path.exists(json_old_path):
                            json_old_filename = os.path.basename(json_old_path)
                            json_new_filename = os.path.basename(json_new_path)
                            
                            rename_plan.append({
                                'old_path': json_old_path,
                                'new_path': json_new_path,
                                'old_filename': json_old_filename,
                                'new_filename': json_new_filename,
                                'subject': subject_id,
                                'session': session_id,
                                'modality': modality,
                                'base_name': base_name,
                                'old_suffix': file_info['suffix'],
                                'new_suffix': f"{i:02d}"
                            })
    
    return rename_plan

def save_rename_plan(rename_plan, output_file):
    """Save the rename plan to a file for review"""
    with open(output_file, 'w') as f:
        f.write("BIDS Naming Fix Rename Plan\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total files to rename: {len(rename_plan)}\n\n")
        
        current_subject = None
        current_session = None
        
        for item in rename_plan:
            if item['subject'] != current_subject:
                current_subject = item['subject']
                f.write(f"\n{current_subject}:\n")
            
            if item['session'] != current_session:
                current_session = item['session']
                f.write(f"  {current_session}:\n")
            
            f.write(f"    {item['modality']}: {item['old_filename']} -> {item['new_filename']}\n")

def execute_renames(rename_plan, dry_run=True):
    """Execute the rename operations"""
    
    if dry_run:
        print("DRY RUN MODE - No files will actually be renamed")
    else:
        print("EXECUTING RENAMES - Files will be renamed!")
    
    success_count = 0
    error_count = 0
    errors = []
    
    for item in tqdm(rename_plan, desc="Renaming files"):
        old_path = item['old_path']
        new_path = item['new_path']
        
        if not os.path.exists(old_path):
            error_msg = f"Source file does not exist: {old_path}"
            errors.append(error_msg)
            error_count += 1
            continue
        
        if os.path.exists(new_path):
            error_msg = f"Target file already exists: {new_path}"
            errors.append(error_msg)
            error_count += 1
            continue
        
        if not dry_run:
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                # Rename the file
                shutil.move(old_path, new_path)
                success_count += 1
            except Exception as e:
                error_msg = f"Error renaming {old_path} -> {new_path}: {str(e)}"
                errors.append(error_msg)
                error_count += 1
        else:
            success_count += 1
    
    return success_count, error_count, errors

def main():
    issues_file = "naming_issues_data.json"
    rename_plan_file = "bids_rename_plan.txt"
    
    print("Loading naming issues data...")
    if not os.path.exists(issues_file):
        print(f"Error: {issues_file} not found. Run analyze_naming_issues.py first.")
        return
    
    issues, stats = load_issues_data(issues_file)
    print(f"Loaded issues for {len(issues)} subjects")
    
    print("Planning renames...")
    rename_plan = plan_renames(issues)
    
    print(f"Generated rename plan for {len(rename_plan)} files")
    
    # Save the plan for review
    save_rename_plan(rename_plan, rename_plan_file)
    print(f"Rename plan saved to: {rename_plan_file}")
    
    # Ask user if they want to proceed
    print("\n" + "="*60)
    print("REVIEW THE RENAME PLAN BEFORE PROCEEDING!")
    print(f"Check {rename_plan_file} to see all planned renames")
    print("="*60)
    
    response = input("\nDo you want to execute the renames? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        print("\nExecuting renames...")
        success, errors, error_list = execute_renames(rename_plan, dry_run=False)
        
        print(f"\nRename complete!")
        print(f"Successfully renamed: {success} files")
        print(f"Errors: {errors} files")
        
        if error_list:
            print("\nErrors encountered:")
            for error in error_list[:10]:  # Show first 10 errors
                print(f"  - {error}")
            if len(error_list) > 10:
                print(f"  ... and {len(error_list) - 10} more errors")
    else:
        print("\nDry run mode - showing first 10 planned renames:")
        success, errors, error_list = execute_renames(rename_plan[:10], dry_run=True)
        print("No files were actually renamed.")

if __name__ == "__main__":
    main()