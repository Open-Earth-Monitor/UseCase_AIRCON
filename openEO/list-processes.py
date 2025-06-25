import openeo

print("Connecting to openeo.cloud...")
# Connect to the back-end
connection = openeo.connect("https://openeo.cloud")
connection.authenticate_oidc()
print("Connection successful.")

# List all available processes
print("Fetching processes...")
processes = connection.list_processes()
print(f"Found {len(processes)} processes.")

# Write them to a file
with open("processes.txt", "w") as f:
    if processes:
        # The items are dictionaries, so we sort by the 'id' key
        sorted_processes = sorted(processes, key=lambda p: p['id'])
        for process in sorted_processes:
            f.write(f"- {process['id']}\n")
        print("Process list written to processes.txt")
    else:
        f.write("No processes found or returned.")
        print("No processes found. Check connection and backend status.")
