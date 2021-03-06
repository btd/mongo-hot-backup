package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	cli "github.com/jawher/mow.cli"
	"github.com/rlmcpherson/s3gof3r"
	log "github.com/sirupsen/logrus"
)

const (
	dateFormat = "2006-01-02T15-04-05"
)

func main() {
	s3gof3r.DefaultConfig.Md5Check = false

	app := cli.App("mongo-hot-backup", "Backup and restore mongodb collections to/from s3 and fs\nBackups are put in a directory structure /<base-dir>/<date>/database/collection")

	connStr := app.String(cli.StringOpt{
		Name:   "mongodb",
		Desc:   "mongodb connection string",
		EnvVar: "MONGODB",
		Value:  "localhost:27017",
	})
	s3domain := app.String(cli.StringOpt{
		Name:   "s3-domain",
		Desc:   "s3 domain",
		EnvVar: "S3_DOMAIN",
		Value:  "s3-eu-west-1.amazonaws.com",
	})
	s3bucket := app.String(cli.StringOpt{
		Name:   "s3-bucket",
		Desc:   "s3 bucket name",
		EnvVar: "S3_BUCKET",
	})
	s3dir := app.String(cli.StringOpt{
		Name:   "s3-base-dir",
		Desc:   "s3 base directory name",
		EnvVar: "S3_DIR",
		Value:  "/backups/",
	})
	accessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key-id",
		Desc:   "AWS Access key id",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	secretKey := app.String(cli.StringOpt{
		Name:      "aws-secret-access-key",
		Desc:      "AWS secret access key",
		EnvVar:    "AWS_SECRET_ACCESS_KEY",
		HideValue: true,
	})
	fsdir := app.String(cli.StringOpt{
		Name:   "fs-base-dir",
		Desc:   "fs base directory name",
		EnvVar: "FS_DIR",
		Value:  "/backups/",
	})
	colls := app.String(cli.StringOpt{
		Name:   "collections",
		Desc:   "Collections to process (comma separated <database>/<collection>)",
		EnvVar: "MONGODB_COLLECTIONS",
		Value:  "foo/content,foo/bar",
	})
	mongoTimeout := app.Int(cli.IntOpt{
		Name:   "mongo-timeout",
		Desc:   "Mongo session connection timeout in seconds. (e.g. 60)",
		EnvVar: "MONGO_TIMEOUT",
		Value:  60,
	})
	rateLimit := app.Int(cli.IntOpt{
		Name:   "rate-limit",
		Desc:   "Rate limit mongo operations in milliseconds. (e.g. 250)",
		EnvVar: "RATE_LIMIT",
		Value:  250,
	})
	batchLimit := app.Int(cli.IntOpt{
		Name:   "batch-limit",
		Desc:   "The size of data in bytes, that a bulk write is writing into mongodb at once. Not recommended to use more than 16MB (e.g. 15000000)",
		EnvVar: "BATCH_LIMIT",
		Value:  15000000,
	})

	app.Command("scheduled-backup", "backup a set of mongodb collections", func(cmd *cli.Cmd) {
		cronExpr := cmd.String(cli.StringOpt{
			Name:   "cron",
			Desc:   "Cron expression for when to run",
			EnvVar: "CRON",
			Value:  "30 10 * * *",
		})
		dbPath := cmd.String(cli.StringOpt{
			Name:   "db-path",
			Desc:   "Path to store boltdb file",
			EnvVar: "DBPATH",
			Value:  "/var/data/mongobackup/state.db",
		})
		run := cmd.Bool(cli.BoolOpt{
			Name:   "run",
			Desc:   "Run backups on startup?",
			EnvVar: "RUN",
			Value:  true,
		})
		healthHours := cmd.Int(cli.IntOpt{
			Name:   "health-hours",
			Desc:   "Number of hours back in time in which healthy backup needs to exist of each named collection for the app to be healthy. (e.g. 24)",
			EnvVar: "HEALTH_HOURS",
			Value:  24,
		})

		cmd.Action = func() {
			parsedColls, err := parseCollections(*colls)
			if err != nil {
				log.Fatalf("error parsing collections parameter: %v", err)
			}
			dbService := newMongoService(*connStr, &labixMongo{}, &defaultBsonService{}, time.Duration(*mongoTimeout)*time.Second, time.Duration(*rateLimit)*time.Millisecond, *batchLimit)
			statusKeeper, err := newBoltStatusKeeper(*dbPath)
			if err != nil {
				log.Fatalf("failed setting up to read or write scheduled backup status results: %v", err)
			}
			defer statusKeeper.Close()
			var storageService storageService
			if *s3bucket != "" {
				storageService = newS3StorageService(*s3bucket, *s3dir, *s3domain, *accessKey, *secretKey)
			} else {
				storageService = newFSStorageService(*fsdir)
			}
			backupService := newMongoBackupService(dbService, storageService, statusKeeper)
			scheduler := newCronScheduler(backupService, statusKeeper)
			healthService := newHealthService(*healthHours, statusKeeper, parsedColls, healthConfig{
				appSystemCode: "up-mgz",
				appName:       "mongobackup",
			})
			httpService := newScheduleHTTPService(scheduler, healthService)
			httpService.ScheduleAndServe(parsedColls, *cronExpr, *run)
		}
	})

	app.Command("backup", "backup a set of mongodb collections", func(cmd *cli.Cmd) {
		dbPath := cmd.String(cli.StringOpt{
			Name:   "db-path",
			Desc:   "Path to store boltdb file",
			EnvVar: "DBPATH",
			Value:  "/var/data/mongobackup/state.db",
		})

		cmd.Action = func() {
			parsedColls, err := parseCollections(*colls)
			if err != nil {
				log.Fatalf("error parsing collections parameter: %v", err)
			}
			dbService := newMongoService(*connStr, &labixMongo{}, &defaultBsonService{}, time.Duration(*mongoTimeout)*time.Second, time.Duration(*rateLimit)*time.Millisecond, *batchLimit)
			statusKeeper, err := newBoltStatusKeeper(*dbPath)
			if err != nil {
				log.Fatalf("failed setting up to read or write scheduled backup status results: %v", err)
			}
			defer statusKeeper.Close()
			var storageService storageService
			if *s3bucket != "" {
				storageService = newS3StorageService(*s3bucket, *s3dir, *s3domain, *accessKey, *secretKey)
			} else {
				storageService = newFSStorageService(*fsdir)
			}
			backupService := newMongoBackupService(dbService, storageService, statusKeeper)
			if err := backupService.Backup(parsedColls); err != nil {
				log.Fatalf("backup failed : %v", err)
			}
		}
	})
	app.Command("restore", "restore a set of mongodb collections", func(cmd *cli.Cmd) {
		dateDir := cmd.String(cli.StringOpt{
			Name:   "date",
			Desc:   "Date to restore backup from",
			EnvVar: "DATE",
			Value:  dateFormat,
		})
		cmd.Action = func() {
			parsedColls, err := parseCollections(*colls)
			if err != nil {
				log.Fatalf("error parsing collections parameter: %v", err)
			}
			dbService := newMongoService(*connStr, &labixMongo{}, &defaultBsonService{}, time.Duration(*mongoTimeout)*time.Second, time.Duration(*rateLimit)*time.Millisecond, *batchLimit)
			if err != nil {
				log.Fatalf("failed setting up to read or write scheduled backup status results: %v", err)
			}
			var storageService storageService
			if *s3bucket != "" {
				storageService = newS3StorageService(*s3bucket, *s3dir, *s3domain, *accessKey, *secretKey)
			} else {
				storageService = newFSStorageService(*fsdir)
			}
			backupService := newMongoBackupService(dbService, storageService, &boltStatusKeeper{})
			if err := backupService.Restore(*dateDir, parsedColls); err != nil {
				log.Fatalf("restore failed : %v", err)
			}
		}
	})

	err := app.Run(os.Args)
	log.Info(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func parseCollections(colls string) ([]dbColl, error) {
	var cn []dbColl
	for _, coll := range strings.Split(colls, ",") {
		c := strings.Split(coll, "/")
		if len(c) != 2 {
			return nil, fmt.Errorf("failed to parse connections string: %s", colls)
		}
		cn = append(cn, dbColl{c[0], c[1]})
	}

	return cn, nil
}
