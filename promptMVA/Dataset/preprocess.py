import json
from distributed import Client, LocalCluster
from coffea.dataset_tools import preprocess

FILES = [
    "DataC_2024.json"
]

def main():
    cluster = LocalCluster(
        n_workers=8,          
        threads_per_worker=1,
        processes=True,
        dashboard_address="127.0.0.1:8787", 
    )
    client = Client(cluster)
    print("Dask dashboard:", client.dashboard_link)

    for file in FILES:
        with open(file) as f:
            fileset = json.load(f)

        runnable, processed = preprocess(
            fileset,
            step_size=200_000,
            skip_bad_files=True,
            scheduler=client
        )
      

        with open(f"processed_{file}", "w") as fout:
            json.dump(processed, fout, indent=2)

        with open(f"runnable_{file}", "w") as fout:
            json.dump(runnable, fout, indent=2)
        
        print("Done:", file)

if __name__ == "__main__":
    try:
        import multiprocessing as mp
        mp.set_start_method("fork", force=True)
    except Exception:
        pass
    main()
