
sudo apt update
sudo apt install python3 python3-pip python3-venv -y

mkdir -p ~/dagster_projects && cd ~/dagster_projects
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install dagster dagster-webserver

dagster project scaffold --name my-dagster-project
cd my-dagster-project

# Start the Dagster web server (UI)
dagster dev


