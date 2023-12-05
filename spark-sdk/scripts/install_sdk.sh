#/bin/bash -ex

# Run from hyperline-developers/spark-sdk:
# ./scripts/install_sdk.sh

NAME=hyperline-0.0.1
PACKAGE_FILENAME=$NAME.tar.gz
ZIP_FILENAME=$NAME.zip 

rm -fr dist/
python -m build
cd dist/
tar xvf $PACKAGE_FILENAME
echo '>> About to Zip'

cd $NAME/src/
zip -r $ZIP_FILENAME hyperline/
cd ../../
mv $NAME/src/$ZIP_FILENAME .

echo ">> Built hyperline package. Uploading to cloud storage.."
gsutil cp $ZIP_FILENAME gs://shared_cluster_stage/hyperline_sdk/

echo ">> Done installing hyperline sdk."
