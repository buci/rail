#!/usr/bin/env bash
## Makes Rail-RNA package downloaded by EC2 nodes in Elastic MapReduce job
## and Rail-RNA installer executable. The former is placed in packages/ and
## the latter is placed in releases/. The version number in src/version.py
## is respected. This script should not be run by Rail-RNA users.
PACKAGES=packages
RELEASES=releases
UPLOAD=0
# Use getopt to determine whether to upload package
while getopts "h?u" opt; do
    case "$opt" in
    h|\?)
        echo "Use -u to upload to WebFaction."
        exit 0
        ;;
    u)  UPLOAD=1
        ;;
    esac
done
cd $(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd src
VER=$(python -c "import version; print version.version_number,")
# Change version number in rail-rna.txt
cd rna/driver
python -c "print '\xe2\x88\x80 Rail-RNA v${VER}'" >rail-rna.txt
cd ../..
# Create installer
INTERMEDIATE=rail-rna_installer.zip
rm -rf ../rail-rna*installer*
zip ../rail-rna_installer.zip $(find . -not -name \*.pyc -not -name .DS\_Store -not -name \*.jar | xargs)
cd ..
mkdir -p $RELEASES
TARGET=${RELEASES}/install_rail-rna-${VER}
rm -rf $TARGET
echo '#!/usr/bin/env python2.7' | cat - $INTERMEDIATE >$TARGET
rm -rf $INTERMEDIATE
chmod 755 $TARGET
cd $RELEASES
FULLRELEASE=$(pwd)
echo "Installer created at ${FULLRELEASE}/install_rail-rna-${VER} ."
if [ $UPLOAD -eq 0 ]
then
	exit 0
fi
echo "Copying to webfactional... ."
scp ${FULLRELEASE}/install_rail-rna-${VER} verve@verve.webfactional.com:/home/verve/webapps/burn1/rail
echo "Installer also available at http://verve.webfactional.com/rail ."
