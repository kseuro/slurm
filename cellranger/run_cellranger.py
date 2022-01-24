import os
import glob
import canine
import pandas as pd

#
# Note:
# Each batch of data will have a unique set of sampleIDs.
# It is therefore necessary to alter the `groups` variable
# to include regex groups that will capture the desired
# sampleIDs.

#
# Set paths to data and desired output directory
data_set = "July2021_set"
root = "/home/kstewart"  # Top level directory
data = f"/data/{data_set}"  # Data directory
out = f"{root}/runs"  # Output directory
#
# Sample sheet - must be generated before running this script
sheet = f"gs://brca-gray/data/{data_set}/sample_sheet.July2021.barcoded.csv"

# Chromium protocol
# Associates a barcode, e.g. SI-GA-B7 with a sequence adapter, e.g. AAACCTCA
chrom = "gs://brca-gray/data/Nov12_set/chromium-shared-sample-indexes-plate.csv"

# Set the data set specific output directory
outDir = f"{out}/{data_set}"

# Set the FastQ Directory
# Can move all of the fastqs into a single directory for this
# See `move_files.sh` for an example script
fastQs = f"{root}{data}/fastqs"

# Set the Genome Reference
hgRef = f"{root}/ref/refdata-cellranger-GRCh38-3.0.0"

#
# Load the sample sheet
try:
    S = pd.read_csv(sheet)
except:
    raise ValueError(f"Sample sheet for {data_set} not found at {sheet}")
# Index column may be read incorrectly, adjust if necessary
if "Unnamed: 0" in S.columns:
    S = pd.read_csv(sheet, index_col=[0])
#
# Load the Chromium protocol sample indexes file
B = pd.read_csv(chrom, header=None)
B = B.rename(columns={0: "barcode"})
B = B.melt(id_vars="barcode", var_name="barcode_no", value_name="barcode_sequence")
#
# Create a DF where the sampleIDs are sorted
# by their corresponding barcode sequence
#
# Read in all the fastq names
L = pd.Series(glob.glob(fastQs + "/*.fastq.gz"))
#
# Regex groups that capture the sampleIDs
# Usually defined on a case-by-case basis
groups = ["(x\d+\w_)", "(x\w+_\d_)"]
subs = list()
#
# Extract each group separately
for group in groups:
    extract = f"{group}(CKD.*)(_[HGKJKCCX2]+)"
    sub = L.str.extract(extract, expand=True)
    sub = sub.drop_duplicates().dropna()
    subs.append(sub)
#
# Recombine groups
L = pd.concat(subs)
#
# Create sample ID from substrings
# Ex: x21004A_ + CKDL210014972-1a-SI_GA_B7 + _HGKJKCCX2
L[0] = L[0] + L[1] + L[2]
L[1] = L[1].str.extract("(SI.*)")  # Chromium barcode
L = L.drop(2, axis=1)
L.columns = ["sampleID", "barcode"]
# Match chromium sheet formatting
L["barcode"] = L["barcode"].str.replace("_", "-")
#
# Merge the sorted samples with the chromium protocol sheet
L = pd.merge(L, B, on="barcode", how="left")
L.loc[:, "Index"] = L["barcode"].str.replace("SI-GA-", "")
L = L.drop("barcode_no", axis=1)
L = L.groupby("Index").agg(" ".join).reset_index()
#
# Merge sample sheet with sorted samples
G = pd.merge(S, L, how="right", on="Index")

cellranger_cmd = (
    "cellranger count --id=${id} "
    + "--transcriptome=${ref} "
    + "--fastqs=${fastQs} "
    + "--sample=${sample} "
    + "--expect-cells=${expect} "
    + "--nosecondary "
    + "--localcores=${nthread}"
)

# Write the Canine .yaml
canine_conf = {
    "retry": 0,
    "name": "cellranger",
    "inputs": {
        "fastQs": fastQs,
        "ref": hgRef,
        "id": list(G["patient"]),
        "sample": list(G["sampleID"].str.replace(" ", ",")),
        "expect": list(G["Target cell recovery"]),
        "nthread": 16,
    },
    "script": ["rm -f */_lock", cellranger_cmd],
    "localization": {"strategy": "NFS", "staging_dir": outDir},
    "resources": {"mem": "100G", "cpus-per-task": 16, "nodes": 1},
}

O = canine.Orchestrator(canine_conf)
result = O.run_pipeline()
print("Fin")