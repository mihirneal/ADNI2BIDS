#!/bin/bash

# This script splits the ADNI DICOM dataset into three separate parts.
# It is designed to be safe, using rsync to copy files without touching the
# original dataset. It also provides progress and timing for each part.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
SOURCE_DIR="/Volumes/CAMRI/adni2bids/dicom"
DEST_BASE_DIR="/Volumes/CAMRI"
NUM_PARTS=3
# ---------------------

echo "Starting dataset split..."
echo "Source directory: $SOURCE_DIR"
echo "Destination base: $DEST_BASE_DIR"
echo "Number of parts: $NUM_PARTS"
echo "--------------------------------------------------"

# Check if the source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source directory '$SOURCE_DIR' not found."
    exit 1
fi

# Create a temporary directory for our subject lists
TMP_DIR=$(mktemp -d)
echo "Using temporary directory for file lists: $TMP_DIR"

# Clean up the temporary directory on script exit
trap 'rm -rf -- "$TMP_DIR"' EXIT

# Get a list of all subject directories
echo "Getting list of subjects..."
ls -1 "$SOURCE_DIR" > "$TMP_DIR/all_subjects.txt"

TOTAL_SUBJECTS=$(wc -l < "$TMP_DIR/all_subjects.txt")
echo "Found $TOTAL_SUBJECTS total subjects."

if [ "$TOTAL_SUBJECTS" -lt "$NUM_PARTS" ]; then
    echo "Error: Fewer subjects than the number of parts to split into."
    exit 1
fi

# Calculate how many subjects go into each part
SUBJECTS_PER_PART=$(( (TOTAL_SUBJECTS + NUM_PARTS - 1) / NUM_PARTS ))
echo "Each part will contain up to $SUBJECTS_PER_PART subjects."

# Split the list of subjects into multiple files
split -l "$SUBJECTS_PER_PART" "$TMP_DIR/all_subjects.txt" "$TMP_DIR/subject_part_"

# --- Main Copying Loop ---
PART_NUM=1
for part_file in $(ls $TMP_DIR/subject_part_*); do
    
    DEST_DIR="$DEST_BASE_DIR/adni2bids_$PART_NUM"
    DEST_DICOM_DIR="$DEST_DIR/dicom"
    
    echo ""
    echo "--- Processing Part $PART_NUM of $NUM_PARTS ---"
    echo "Destination: $DEST_DIR"
    
    # Create the destination directories
    mkdir -p "$DEST_DICOM_DIR"
    
    # Time the entire copy operation for this part.
    time (
        # Loop through each subject in the part file and copy it individually
        # for maximum reliability.
        while IFS= read -r subject_dir; do
            # Skip any potential empty lines in the file
            if [ -z "$subject_dir" ]; then
                continue
            fi
            # Copy the individual subject directory. The source path is now explicit.
            rsync -ah --progress "$SOURCE_DIR/$subject_dir" "$DEST_DICOM_DIR/"
        done < "$part_file"
    )

    echo "✅ Part $PART_NUM completed."
    
    ((PART_NUM++))
done

echo ""
echo "--------------------------------------------------"
echo "🎉 All parts have been copied successfully!"
echo "Your datasets are located in:"
for i in $(seq 1 $NUM_PARTS); do
    echo "  - $DEST_BASE_DIR/adni2bids_$i"
done
