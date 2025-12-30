import pathlib
import clickhouse_connect

OUT_PATH = pathlib.Path("results/result.csv")
SQL_PATH = pathlib.Path("sql/02_query.sql")

def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    client = clickhouse_connect.get_client(host="localhost", port=8123,
                                           username="click", password="click",
                                           database="teta")
    sql = SQL_PATH.read_text(encoding="utf-8")
    df = client.query_df(sql)
    df.to_csv(OUT_PATH, index=False)
    print(f"saved: {OUT_PATH}")


if __name__ == "__main__":
    main()
