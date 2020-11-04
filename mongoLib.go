package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoLib interface {
	DialWithTimeout(url string, timeout time.Duration) (mongoSession, error)
}

type labixMongo struct{}

func (m *labixMongo) DialWithTimeout(url string, timeout time.Duration) (mongoSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		cancel()
		return nil, err
	}

	return &labixSession{client, ctx, cancel}, nil
}

type mongoSession interface {
	Close() error
	SnapshotIter(database, collection string) (mongoIter, error)
	RemoveAll(database, collection string, removeQuery interface{}) error
	Bulk(database, collection string) mongoBulk
}

type labixSession struct {
	client *mongo.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *labixSession) Close() error {
	s.cancel()
	err := s.client.Disconnect(s.ctx)
	return err
}

func (s *labixSession) SnapshotIter(database, collection string) (mongoIter, error) {
	cursor, err := s.client.Database(database).Collection(collection).Find(s.ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(s.ctx)
	return &labixIter{cursor, s.ctx}, nil
}

func (s *labixSession) RemoveAll(database, collection string, removeQuery interface{}) error {
	_, err := s.client.Database(database).Collection(collection).DeleteMany(s.ctx, nil)
	return err
}

func (s *labixSession) Bulk(database, collection string) mongoBulk {
	return &labixBulk{s.client, s.ctx, database, collection, []mongo.WriteModel{}}
}

type mongoBulk interface {
	Run() error
	Insert(data []byte)
}

type labixBulk struct {
	client      *mongo.Client
	ctx         context.Context
	database    string
	collection  string
	writeModels []mongo.WriteModel
}

func (b *labixBulk) Run() error {
	_, err := b.client.Database(b.database).Collection(b.collection).BulkWrite(b.ctx, b.writeModels)
	return err
}

func (b *labixBulk) Insert(data []byte) {
	b.writeModels = append(b.writeModels, mongo.NewInsertOneModel().SetDocument(data))
}

type mongoIter interface {
	Next() ([]byte, bool)
	Err() error
}

type labixIter struct {
	cursor *mongo.Cursor
	ctx    context.Context
}

func (i *labixIter) Next() ([]byte, bool) {
	hasNext := i.cursor.TryNext(i.ctx)
	result := i.cursor.Current

	return result, hasNext
}

func (i *labixIter) Err() error {
	return i.cursor.Err()
}
