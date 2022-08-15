package repository

import (
	"context"

	"log"

	glog "github.com/labstack/gommon/log"

	"github.com/Egor-Tihonov/Kafka-proj/internal/models"
	"github.com/jackc/pgx/v4/pgxpool"
)

type PostgresR struct {
	Pool *pgxpool.Pool
}

func NewConnection() (*PostgresR, error) {
	conn, err := pgxpool.Connect(context.Background(), "postgresql://postgres:123@localhost:5432/postgres")
	if err != nil {
		log.Print("failed connect to postgres")
		return nil, err
	}
	log.Print("successfully connect to postgres...")
	return &PostgresR{Pool: conn}, nil
}

func (p *PostgresR) AddToDB(ctx context.Context, value []models.Message) error {
	for _, v := range value {
		_, err := p.Pool.Exec(ctx, "insert into tablekafka(message) values($1)", v.NewMessage)
		if err != nil {
			glog.Errorf("database error with add message: %v", err)
			return err
		}
	}

	return nil
}
