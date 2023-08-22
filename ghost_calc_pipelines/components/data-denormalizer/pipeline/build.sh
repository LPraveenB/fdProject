#!/bin/bash

ls component/data-ingestion/src/*.py
mydir=component/data-denormalizer/src/
files="component/data-denormalizer/src/*"
for f in $files;do
        echo "The variable's name is $f"
        #echo "The variable's content is ${!f}"
        gsutil cp $f gs://vertex-scripts/de-scripts/data-denormalizer/
done

gsutil cp component/data-denormalizer/resources/*.json $f gs://vertex-scripts/de-scripts/data-denormalizer/resources/
