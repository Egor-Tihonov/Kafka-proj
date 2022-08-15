package repository

import (
	"context"

	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	glog "github.com/labstack/gommon/log"
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

func (p *PostgresR) AddToDB(ctx context.Context, batch *pgx.Batch) error {
	result := p.Pool.SendBatch(ctx, batch)
	_, err := result.Exec()
	if err != nil {
		glog.Errorf("database error %e", err)
		return err
	}
	err = result.Close()
	if err != nil {
		glog.Errorf("database error %e", err)
		return err
	}

	return nil
}
