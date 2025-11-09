# S3_upload
Authenticate &amp; review upload to S3 

repository structure
S3_upload
├── .git/            <-- Main project's Git repository  
├── app.py  
├── templates/
    └── index.html
└── upload.py      <-- This folder also contains a .git directory  
└── to_S3.py       <-- PROBLEM: The nested Git repository  

There are 2 workflows:
1. upload to S3 temp
   uploads files to the temp folder in the S3 bucket
      -requires bucket-level authorization (authorize through aws-cli)
      -run app.py
3. upload to S3 main
       -requires encryption key at folder level
       -run to_S3.py
