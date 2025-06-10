# Hail
Merge gVCF files into a new or existing VDS and perform sample level QC (blue), partition VDS into MatrixTables by Chr with variant level QC (green) using Hail. 

gVCF-to-VDS Pipeline

This repository contains a Hail-based pipeline to merge a large number of gVCF files into a single Variant Dataset (VDS), compute sample-level QC, and persist the results.

⸻

Table of Contents
	1.	Prerequisites
	2.	Environment Setup
	3.	Repository Structure
	4.	Inputs
	5.	Running the Pipeline
	•	Using the Python Script Directly
	•	Submitting with PBS (qsub.sh)
	6.	Configuration
	7.	Outputs
	8.	Troubleshooting
	9.	License

⸻

Prerequisites
	•	HPC or cluster access with support for PBS/Torque (or modify qsub.sh for your scheduler).
	•	Conda (e.g., Miniforge3) for environment management.
	•	Git to clone this repository.
	•	Python 3.9+, as specified in environment.yml.

⸻

Environment Setup
	1.	Clone the repository:

git clone https://github.com/<your-org>/<your-repo>.git
cd <your-repo>


	2.	Create the Conda environment:

conda env create --file environment.yml


	3.	Activate the environment:

conda activate hail



This installs Hail, its Spark dependencies, and any other Python packages needed by the pipeline.

⸻

Repository Structure

├── environment.yml         # Conda environment spec
├── gVCF_to_VDS.py          # Main pipeline script
├── qsub.sh                 # PBS submission wrapper
├── test.txt                # Example list of gVCF file paths
├── README.md               # This documentation
└── outputs/                # Directory where outputs will be written
    ├── test.vds            # Merged VDS
    └── test_sample_qc.ht   # Sample QC Hail Table

Note: adjust paths in the scripts if you change your working directories.

⸻

Inputs
	•	test.txt: Plain text list of absolute paths to gVCF files (one per line).
	•	gVCF files: Ensure each has an accompanying .tbi index.


⸻

Running the Pipeline

Using the Python Script Directly
	1.	Ensure the Conda env is active:

conda activate hail


	2.	Edit gVCF_to_VDS.py to set your paths and parameters (see Configuration).
	3.	Run the script:

python gVCF_to_VDS.py



Submitting with PBS (qsub.sh)
	1.	Make sure qsub.sh is executable:

chmod +x qsub.sh


	2.	Submit to the scheduler:

qsub qsub.sh


	3.	Monitor job output in the files specified by #PBS -o and #PBS -e.

⸻

Configuration

Edit the top of gVCF_to_VDS.py to adjust:

# Resources
ncpu = "local[20]"          # Spark master URL (e.g., local[<cores>])
ram = "140g"                  # Memory per driver/executor

# Combiner settings
BF = 15                        # branch_factor (fan-in of merge tree)
GBS = 10                       # gVCF batch size (files per merge task)

tmp_dir = '<your_tmp_dir>'    # Hail scratch space
tmp_local = '<your_spark_local>'  # spark.local.dir
combiner_tmp = '<combiner_tmp>'   # combiner plan + intermediates
save_path = os.path.join(combiner_tmp, 'combiner_plan.json')

# File paths
gvcf_txt = 'test.txt'         # List of gVCFs to merge
output_vds = 'outputs/test.vds'
sample_qc = 'outputs/test_sample_qc.ht'

	•	target_records in new_combiner controls partition size (e.g., 150_000).
	•	You can supply use_genome_default_intervals=True or specify gvcf_import_intervals for exact control.

⸻

Outputs
	•	outputs/test.vds/: Directory containing the merged VDS.
	•	outputs/test_sample_qc.ht: Hail Table of per-sample QC metrics.

Inspect with:

import hail as hl
mt = hl.read_matrix_table('outputs/test.vds')
hl.read_table('outputs/test_sample_qc.ht').show()


⸻

Troubleshooting
	•	File handle limits: If you see “too many open files” errors, reduce gvcf_batch_size to ≤ your system’s ulimit.
	•	Memory errors: Adjust ram and ncpu to fit available nodes.
	•	Spark UI: Visit http://<driver-host>:4040 to monitor job stages.



![Hail Workflow](Hail_workflow.png)

