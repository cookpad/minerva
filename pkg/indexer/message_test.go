package indexer_test

/*

func TestLoadIndexes(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_S3_REGION")
	bucket := os.Getenv("MINERVA_TEST_S3_BUCKET")
	key := os.Getenv("MINERVA_TEST_S3_KEY")

	if region == "" || bucket == "" || key == "" {
		t.Skip("MINERVA_TEST_S3_{REGION,BUCKET,KEY} are not set")
	}

	ch := indexer.LoadMessage(indexer.NewS3Loc(region, bucket, key))
	var records []interface{}

	for q := range ch {
		records = append(records, &q.Timestamp)
	}
	assert.NotEqual(t, 0, len(records))
}
*/
