# run_scrapers.py
import argparse
import os
import subprocess

def run_scraper(script_name, args=None):
    """Runs a Python script as a subprocess."""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    cmd = ["python", script_path]
    if args:
        cmd.extend(args)
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("Error:")
        print(result.stderr)
        return False
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run F1 data scrapers.")
    parser.add_argument("--races", action="store_true", help="Run the races scraper.")
    parser.add_argument("--results", action="store_true", help="Run the results scraper.")
    parser.add_argument("--year", type=int, nargs='+', help="Years to scrape (e.g., --year 2010 2012).")

    args = parser.parse_args()

    # Get the year range from the command-line arguments.
    years = []
    if args.year:
        years = [str(y) for y in args.year]
    
    # Run the selected scraper(s).
    if args.races:
        if not years:
            print("Please provide at least one year with --year when running --races.")
        else:
            run_scraper("races.py", args=years)
            
    if args.results:
        # The results scraper does not need year arguments as it queries the DB.
        run_scraper("results.py")
    
    if not (args.races or args.results):
        print("No scraper selected. Use --races, --results, or both.")
