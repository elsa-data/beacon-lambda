aws lambda invoke --function-name elsa-data-beacon --cli-binary-format raw-in-base64-out --payload '{"vcf_bucket":"umccr-10g-data-dev", "vcf_key": "HG00174/HG00174.hard-filtered.vcf.gz", "vcf_index_bucket": "umccr-10g-data-dev", "vcf_index_key": "HG00174/HG00174.hard-filtered.vcf.gz.tbi", "reference_name": "chr1", "start": 1220751, "reference_bases": "T", "alternate_bases": "C"}' output.json
cat output.json | jq .

