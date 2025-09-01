# HAIL on Imperial HPC

This repository provides scripts to run [Hail](https://hail.is/) workflows on the Imperial College High Performance Computing (HPC) cluster, specifically for:

- Converting multiple single-sample gVCF files into a multi-sample [VDS](https://hail.is/docs/0.2/methods/impex.html#hail.VariantDataset) (Variant Dataset) using the Hail Combiner.

---

## Walkthrough

### 1. 🔧 Setup

#### a) Clone this repository
```bash
git clone https://your.repo.url/here.git
cd Hail  # or the name of your cloned directory
```

#### b) Create the Conda environment

This repository uses a predefined Conda environment (`conda_env.yml`) to ensure all dependencies are consistent. The environment must be created on the **login node** of the Imperial HPC.

See [Imperial HPC Conda guide](https://wiki.imperial.ac.uk/display/HPC/Conda) if needed.

```bash
# Enable conda in your shell (adjust the path if needed)
eval "$(~/anaconda3/bin/conda shell.bash hook)"

# Remove existing environment (if it exists)
conda env remove -n hail

# Create the environment from the provided file
conda env create --file conda_env.yml
```

### 2. Convert gVCF to VDS

This section describes how to convert a list of single-sample gVCF files into a Hail Variant Dataset (VDS).

#### a) Prepare a list of gVCFs

Create a text file where each line is the **absolute path** to a `.gvcf.gz` file you want to include in the multi-sample dataset.

Example (`my_gvcf_list.txt`):
```
/rds/general/project/example/data/sample01.gvcf.gz
/rds/general/project/example/data/sample02.gvcf.gz
```

#### b) Configure your run

Edit the `set_variables.sh` file to define:

- File paths (gVCF list, output VDS, logs, etc.)
- Runtime parameters (threads, memory, etc.)

Each variable is documented in the file to help guide configuration.

#### c) Submit your job

Use the provided submission script to run the pipeline. You must pass the **absolute path** to your `set_variables.sh` file:

```bash
bash scripts/submit_gVCF_to_VDS.sh /full/path/to/set_variables.sh
```