## Objective:
Create a select by table with records

1) Create a virtual environment with a command like: python -m venv venv
2) Activate the enviroment with a command like: venv/Scripts/activate.bat
3) pip3 install --upgrade pip
4) python -m pip install --upgrade setuptools
5) pip3 install -r requirements.txt
   
# Create requirements
pip3 freeze > requirements.txt

# Remove all
pip3 uninstall -r requirements.txt -y

# How to run

To run execute the command python main.py passing the parameters like: 
python main.py --project project_id --dataset_id dataset_id --table_id table_id
