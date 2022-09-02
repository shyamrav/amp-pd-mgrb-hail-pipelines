import hail as hl
hl.init(default_reference='GRCh38',
       spark_conf={
           'spark.hadoop.fs.gs.requester.pays.mode': 'CUSTOM',
           'spark.hadoop.fs.gs.requester.pays.buckets': 'hail-datasets-us',
           'spark.hadoop.fs.gs.requester.pays.project.id': '1050631177138'})

mt = hl.read_matrix_table('gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.sparse.mt/')

# The matrix we start with is a sparse matrix. Densifying before filtering. 
mt = hl.experimental.densify(mt)

# removing reference blocks from matrix after densify
mt = mt.filter_rows(hl.agg.any(mt.GT.is_non_ref()))

# Filter out rows with less than 10 DP
# Rows with missing values are removed regardless of keep
mt = mt.filter_rows(mt.info.DP > 10, keep=True)

# Filter by PASS status
mt = mt.filter_rows(mt.filters == {"PASS"})

# Filter by Q values greater than 90
mt = mt.filter_rows(mt.info.QUALapprox > 90)

# Hardy-Weinberg equilibrium greater than 0.001
mt = mt.annotate_rows(hwe = hl.agg.hardy_weinberg_test(mt.GT))
mt = mt.filter_rows(mt.hwe.p_value > 0.001)

# Run variant QC 
mt = hl.variant_qc(mt)
# Filter by call_rate lower than 0.9
mt = mt.filter_rows(mt.variant_qc.call_rate > 0.9)

# Annotate matrix with dbNSFP annotations
db = hl.experimental.DB(region='us', cloud='gcp')
mt = db.annotate_rows_db(mt, 'dbNSFP_variants')

# Save filtered Matrix separately
output_path = "gs://activestorage_amppd_mgrb/v1-2-amp-pd-mgrb.dense.filtered.annotated.mt/"
mt.write(output_path, overwrite=True)
