package merger_test

/*
func dumpMessageParquet(rows []internal.MessageRecord) string {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		log.Fatal(err)
	}
	fd.Close()
	filePath := fd.Name()

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(internal.MessageRecord), 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pw.WriteStop()

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := range rows {
		if err := pw.Write(rows[i]); err != nil {
			log.Fatal(err)
		}
	}

	return filePath
}

func loadMessageParquet(fpath string) []internal.MessageRecord {
	batchSize := 2
	buf := make([]internal.MessageRecord, batchSize)
	var rows []internal.MessageRecord

	fr, err := local.NewLocalFileReader(fpath)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(internal.MessageRecord), 4)
	if err != nil {
		log.Fatal(err)
	}
	defer pr.ReadStop()

	for i := 0; int64(i) < pr.GetNumRows(); i += batchSize {
		if err := pr.Read(&buf); err != nil {
			log.Fatal(err)
		}
		rows = append(rows, buf...)
	}

	return rows
}

func TestHandlerMessage(t *testing.T) {
	c1path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 1, Timestamp: 100, Seq: 0, Message: "not"},
		{ObjectID: 1, Timestamp: 100, Seq: 1, Message: "sane"},
	})
	c2path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 2, Timestamp: 100, Seq: 1, Message: "five"},
		{ObjectID: 2, Timestamp: 100, Seq: 2, Message: "timeless"},
		{ObjectID: 2, Timestamp: 100, Seq: 3, Message: "words"},
	})
	c3path := dumpMessageParquet([]internal.MessageRecord{
		{ObjectID: 2, Timestamp: 100, Seq: 1, Message: "fifth"},
		{ObjectID: 2, Timestamp: 100, Seq: 2, Message: "magic"},
	})

	defer os.Remove(c1path)
	defer os.Remove(c2path)
	defer os.Remove(c3path)

	dummyS3 := dummyS3ClientMessage{
		c1: mustOpen(c1path),
		c2: mustOpen(c2path),
		c3: mustOpen(c3path),
	}
	internal.TestInjectNewS3Client(&dummyS3)
	defer internal.TestFixNewS3Client()

	args := main.NewArgument()
	args.Queue = internal.MergeQueue{
		Schema: internal.ParquetSchemaMessage,
		SrcObjects: []internal.S3Location{
			{Region: "t1", Bucket: "b1", Key: "c1.parquet"},
			{Region: "t2", Bucket: "b1", Key: "c2.parquet"},
			{Region: "t2", Bucket: "b1", Key: "c3.parquet"},
		},
		DstObject: internal.S3Location{
			Region: "t1",
			Bucket: "b1",
			Key:    "k3.parquet",
		},
	}

	err := main.MergeParquet(args)
	require.NoError(t, err)
	rows := loadMessageParquet(dummyS3.dumpFile)
	assert.Equal(t, 7, len(rows))

	hasV5term := false
	for _, r := range rows {
		if r.Message == "five" {
			hasV5term = true
			break
		}
	}
	assert.True(t, hasV5term)

	assert.Equal(t, "b1", dummyS3.deletedBucket)
	assert.Equal(t, 3, len(dummyS3.deletedObjects))
	assert.Contains(t, dummyS3.deletedObjects, "c1.parquet")
	assert.Contains(t, dummyS3.deletedObjects, "c2.parquet")
	assert.Contains(t, dummyS3.deletedObjects, "c3.parquet")
}
*/
