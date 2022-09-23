# amp-pd-mgrb-hail-pipelines

Repo to store code relating to Hail filtering, QC and analysis. 

### Running Varient QC and dbNSFP
```
usage: 00_varient_quality_control.py [-h] [-f] [-w] -i INPUT_MT_PATH -o
                                     OUTPUT_MT_PATH
                                     [-p REQUESTER_PAYS_PROJECT_ID]

optional arguments:
  -h, --help            show this help message and exit
  -f, --full_run        Runs on chr22 and chrX only by default. If full_run is
                        set, it runs on the whole matrix. WARNING: This will
                        be VERY expensive
  -w, --overwrite       If set will overwrite output matrix if it already
                        exists

required named arguments:
  -i INPUT_MT_PATH, --input_mt_path INPUT_MT_PATH
  -o OUTPUT_MT_PATH, --output_mt_path OUTPUT_MT_PATH
  -p REQUESTER_PAYS_PROJECT_ID, --requester_pays_project_id REQUESTER_PAYS_PROJECT_ID
                        Project ID to bill to when accessing requester pays
                        bucket, needed to access hail annotationDB
```

### Running Hard-filtering
```
usage: 01_hard_filter.py [-h] [-f] [-w] [-v] -i INPUT_MT_PATH -o
                         OUTPUT_MT_PATH

optional arguments:
  -h, --help            show this help message and exit
  -w, --overwrite       If set will overwrite output matrix if it already
                        exists
  -v, --verbose         Print some sanity checking. WARNING: may only want to
                        use for subset data. May be expensive when running on
                        whole matrix.

required named arguments:
  -i INPUT_MT_PATH, --input_mt_path INPUT_MT_PATH
                        Requires path to a Dense matrix
  -o OUTPUT_MT_PATH, --output_mt_path OUTPUT_MT_PATH
```

### Run SDT Test
```
usage: 02_sdt_test.py [-h] -i INPUT_MT_PATH -o OUTPUT_MT_PATH -f PEDGREE_FILE

optional arguments:
  -h, --help            show this help message and exit

required named arguments:
  -i INPUT_MT_PATH, --input_mt_path INPUT_MT_PATH
                        Requires path to a Dense hard filtered matrix
  -o OUTPUT_MT_PATH, --output_mt_path OUTPUT_MT_PATH
  -f PEDGREE_FILE, --pedgree_file PEDGREE_FILE
```
 
