import hail as hl
hl.init(default_reference='GRCh38')

mt = hl.read_matrix_table('gs://activestorage_amppd_mgrb/v1-1-amp-pd-mgrb.mt/')

# We split using this function since the combiner outputs a sparse matrix table
mt = hl.experimental.sparse_split_multi(mt)

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

# Filter by call_rate lower than 0.9
mt = mt.filter_rows(mt.variant_qc.call_rate > 0.9)


# Annotate matrix with dbNSFP annotations
db = hl.experimental.DB(region='us', cloud='gcp')
mt = db.annotate_rows_db(mt, 'dbNSFP_variants')

# Save filtered Matrix separately
output_path = "gs://activestorage_amppd_mgrb/v1-1-amp-pd-mgrb.filtered.mt/"
mt.write(output_path)
