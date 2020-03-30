# This script generate the SLURM and launch the assembly using the cluster for all files present in the input folder.
# The files must be paired (both R1 and R2 files), in a fastq format

import os

# Functions

def create_slurm(read1, read2, name):
	if not "input/" in read1:
		read1 = "input/" + read1
	if not "input/" in read2:
		read2 = "input/" + read2

	if not os.path.exists(read1):
		print(f"Could not open the R1 file : {read1}")
	if not os.path.exists(read2):
		print(f"Could not open the R2 file : {read2}")

	print()
	print(f"Creating slurm file {name}_slurm")
	with open(name+"_slurm", 'w') as f:
		f.write("#!/bin/bash\n")
		f.write(f"#SBATCH --job-name=Phage_assembly_{name}\n")
		f.write("#SBATCH --cpus-per-task=16\n")
		f.write("#SBATCH --mem-per-cpu=1G\n")
		f.write("#Total memory reserved: 16GB\n")
		f.write("#SBATCH --time=00:30:00\n")
		f.write("#SBATCH --qos=30min\n")
		f.write(f"#SBATCH --output=Phage_assembly_{name}.out\n")
		f.write("\n")
		f.write("\n")
		f.write("#Sub-sampling of raw files\n")
		f.write("\n")
		f.write("module load Seqtk\n")
		f.write("echo\n")
		f.write("echo === Sub_sampling ===\n")
		f.write("\n")
		f.write(f"cp {read1} $TMPDIR/file1\n")
		f.write(f"cp {read2} $TMPDIR/file2\n")
		f.write("\n")
		f.write(f"seqtk sample -s100 $TMPDIR/file1 150000 > $TMPDIR/file1_subsampled\n")
		f.write(f"seqtk sample -s100 $TMPDIR/file2 150000 > $TMPDIR/file2_subsampled\n")
		f.write("\n")
		f.write("mkdir -p sub_sampled\n")
		f.write(f"cp $TMPDIR/file1_subsampled sub_sampled/{name}_R1_subsampled.fastq\n")
		f.write(f"cp $TMPDIR/file2_subsampled sub_sampled/{name}_R2_subsampled.fastq\n")
		f.write("\n")
		f.write("echo Done\n")
		f.write("echo\n")
		f.write("module purge\n")
		f.write("\n")
		f.write("# Running Unicycler on the sub sampled files\n")
		f.write("\n")
		f.write("echo\n")
		f.write("echo === Running Unicycler ===\n")
		f.write("\n")
		f.write("module load Unicycler\n")
		f.write(f"unicycler --threads $SLURM_CPUS_PER_TASK \
			-1 sub_sampled/{name}_R1_subsampled.fastq \
			-2 sub_sampled/{name}_R2_subsampled.fastq \
			-o Unicycler_output/{name} \
			--mode bold \
			--min_fasta_length 5000 \n")
		f.write("\n")
		f.write(f"cp Unicycler_output/{name}/assembly.fasta output/{name}.fasta\n")
		f.write("\n")
		f.write("echo Done\n")
		f.write("echo\n")
		f.write("\n")
		f.write("exit")
	print("Slurm file created")

def run_slurm(slurm_file):
	print(f"Sending job {slurm_file} to the cluster.")
	os.system(f"sbatch {slurm_file}")
	name = slurm_file.split("_")[0]
	print(f"Output from the cluster are written in the file Phage_assembly_{name}.out")
	print()


# Main code

if __name__ == "__main__":

	# Check that the input and output folder exists

	if not os.path.exists("input"):
		print("Did not find the input folder. Are you in the right Unicycler folder ?")

	if os.path.exists("output"):
		print("Removing content of the output folder.")
		os.system("rm -f output/*")
	else:
		print("Creating output folder.")
		os.mkdir("output")


	file_list = os.listdir("input")
	while len(file_list) > 0:
		name = file_list[0].split("_")[0]
		names = [tmp for tmp in file_list if name in tmp]
		
		create_slurm(names[0], names[1], name)
		run_slurm(f"{name}_slurm")

		file_list.remove(names[0])
		file_list.remove(names[1])