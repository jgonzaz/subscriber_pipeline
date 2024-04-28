#!/bin/bash

# Asking user if they want to start running cleaning script
echo "Ready to clean the data? [T/F]"
read cleandecision

if [ "$cleandecision" == "T" ]
then
  # starting cleaning process
  echo "Cleaning Date"
  python dev/script_ingest_data.py
  echo "Cleaning Complete"

  # setting variables to compare changelog of prod and dev
  dev_version=$(head -n 1 dev/changelog.md)
  prod_version=$(head -n 1 prod/changelog.md)

  read -a splitversion_dev <<< "$dev_version"
  read -a splitversion_prod <<< "$prod_version"

  dev_version=${splitversion_dev[1]}
  prod_version=${splitversion_prod[1]}

  if [ "$prod_version" != "$dev_version" ]
  then
    # setting value for script continue
    echo "New changes present. Move files to prod? [T/F]"
    read scriptcontinue
  else
    echo "No changes detected"
    scriptcontinue="F"
  fi
else
 echo "Come back when you are ready"
fi

if [ $scriptcontinue == "T" ]
then
  # moving files from dev to prod
  for filename in dev/*
  do
    # filtering what files to copy
    if [ "$filename" == "dev/cademycode_clean.db" ] || [ "$filename" == "dev/student_info_cleansed.csv" ] || [ "$filename" = "dev/changelog.log" ]
    then
      cp "$filename" prod
      echo "Copying" "$filename"
    else
      echo "Not copying" "$filename"
    fi
  done
elif [ $scriptcontinue == "F" ]
  then
    echo "No changes detected. Files not copied to prod"
else
  echo "Invalid input"
fi

