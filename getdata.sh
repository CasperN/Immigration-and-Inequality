mkdir Data/
cd Data

echo
echo Downloading ACS data 2007 - 2015
echo

for i in 07 08 09 10 11 12 13 14 15;
do
    wget https://www2.census.gov/programs-surveys/acs/data/pums/20$i/1-Year/csv_pus.zip
done

echo
echo Unzipping data

unzip csv_pus.zip
for i in 2 3 4 5 6 7 8;
do
    unzip csv_pus.$i &
done

echo
echo moving pdf and zip files

mkdir READMEs
mv *.pdf READMEs/
mkdir zips
mv *.zip* zips

echo
echo Using Python to clean data and moving csvs

python3 ../cleanData.py ACSdata.csv
mkdir csvs
mv *.csv csvs
mv csvs/ACSdata.csv .

echo
echo Done!
