import polars as pl
import polars.selectors as cs


def main():
    
    polars_obj = pl.read_csv('src/wines_SPA.csv', infer_schema_length=None)
    
    
    polars_obj = polars_obj.with_columns(
        cs.string().str.to_decimal()
    )

    print(polars_obj.schema)
    
    polars_obj.write_parquet("src/data.parquet")
    

if __name__=="__main__":
    main()
    