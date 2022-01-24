#!/bin/bash
name="July2021_set"

for i in `ls -1 runs/${name}/jobs`; do
	#
	# Move the CellRanger output to the Google Bucket
	sample=`ls -1 runs/${name}/jobs/${i}/workspace/ | grep -i MGH`
	if [ "$sample" == "" ]; then
		sample=`ls -1 runs/${name}/jobs/${i}/workspace/ | grep -i GRB`
	fi
	outputs="runs/${name}/jobs/${i}/workspace/${sample}/outs"
	bucket="gs://brca-gray-data/${name}/cellranger/${sample}"
	websumm="gs://brca-gray-data/${name}/cellranger/${sample}/web_summary.html"
	webBucket="gs://brca-gray-data/${name}/cellranger_html/${sample}.html"
	#
	# Move outputs to Google Bucket
	gsutil -m cp -r ${outputs} ${bucket}
	#
	# Consolidate the .html reports in a single directory
	gsutil cp ${websumm} ${webBucket}
done
