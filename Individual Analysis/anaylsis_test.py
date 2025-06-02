# messin.py
import time
import duckdb as dd
import glob
import os

def main():

    start_time = time.perf_counter_ns()

    con = dd.connect()

    parquet_dir = os.path.join(os.path.dirname(__file__), "data", "*.parquet")
    parquet_files = glob.glob(parquet_dir)
    if not parquet_files:
        print(f"No Parquet files found in directory: {parquet_dir}")
        return  # Exit if no files are found.

    parquet_path = ", ".join([f"'{file}'" for file in parquet_files])

    # Create a view that combines all Parquet files.
    con.execute(f"CREATE VIEW lichess_data AS SELECT * FROM read_parquet([{parquet_path}])")


    see_row = """
        SELECT *
        FROM lichess_data
        LIMIT 1;
    """
    see_result = con.execute(see_row).fetchall() # Use fetchall() to get all limited rows


    # Most Common Openings (Top 20)
    query_most_common_openings = """
        SELECT Opening, COUNT(*) AS OpeningCount
        FROM lichess_data
        GROUP BY Opening
        ORDER BY OpeningCount DESC
        LIMIT 20
    """
    most_common_openings = con.execute(query_most_common_openings).fetchall()



    # Average Elo of White and Black
    query_avg_elo = """
       SELECT AVG(WhiteElo) AS AvgWhiteElo, AVG(BlackElo) AS AvgBlackElo
       FROM lichess_data
    """
    avg_elo = con.execute(query_avg_elo).fetchone()


    # 3. Most common result
    query_result = """
        SELECT Result, COUNT(*) AS ResultCount
        FROM lichess_data
        GROUP BY Result
        ORDER BY ResultCount DESC
        LIMIT 1
    """
    most_common_result = con.execute(query_result).fetchone()

    # 4. Number of games in the dataset
    query_num_games = "SELECT COUNT(*) FROM lichess_data"
    num_games = con.execute(query_num_games).fetchone()[0]


    # 5. Filtered by Termination type
    query_termination = """
        SELECT Termination, COUNT(*) AS TerminationCount
        FROM lichess_data
        GROUP BY Termination
        ORDER BY TerminationCount DESC
        LIMIT 1
    """
    most_common_termination = con.execute(query_termination).fetchone()



    end_time = time.perf_counter_ns()
    elapsed_time = end_time - start_time


    # --- Output Results ---

    print(f"Analysis of Lichess Data ({parquet_dir}):")
    print(f"  Total Games: {num_games:,}")
    print(f"  Execution Time: {elapsed_time / 1_000_000:.2f} ms")

    print("  Top 20 Most Common Openings:")
    if most_common_openings:
        for opening, count in most_common_openings:
            print(f"    - {opening} (Count: {count:,})")
    else:
        print("    No opening data found.")


    if avg_elo:
        print(f"  Average White Elo: {avg_elo[0]:.2f}")
        print(f"  Average Black Elo: {avg_elo[1]:.2f}")
    else:
        print("  No Elo data found.")

    if most_common_result:
        print(f"  Most Common Result: {most_common_result[0]} (Count: {most_common_result[1]:,})")
    else:
        print("  No result data found.")
    if most_common_termination:
        print(f" Most common Termination: {most_common_termination[0]} (Count:{most_common_termination[1]:,})")
    else:
        print("  No termination data found")

if __name__ == "__main__":
    main()