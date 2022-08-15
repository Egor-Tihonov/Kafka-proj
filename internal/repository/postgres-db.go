package repository

import (
	"context"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type PostgresR struct {
	Pool *pgxpool.Pool
}

func NewConnection() (*PostgresR, error) {
	conn, err := pgxpool.Connect(context.Background(), "postgresql://postgres:123@localhost:5432/postgres")
	if err != nil {
		log.Println("failed connect to postgres")
		return nil, nil
	}
	log.Println("successfully connect to postgres")
	return &PostgresR{Pool: conn}, nil
}

func (p *PostgresR) AddToDB(ctx context.Context, value *pgx.Batch) error {
	rps_result := p.Pool.SendBatch(ctx, value)
	rps_result.Exec()
	err := rps_result.Close()
	if err != nil {
		log.Printf("error with batch, %e", err)
		return err
	}
	return nil
}
