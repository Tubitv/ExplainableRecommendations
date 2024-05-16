
# Explainable Recommendations

This project has been voted the best open-source project from the Tubi 2024 Hackathon event 🏆.

Giving explanation to recommendations could gain users' trust and help convert a recommendation to a viewership.

This project creates a service to provide explanations for given item ids and device id.

This project works no matter which models are used to generate those recommendations.



## How to run

### Setup
```commandline
conda create --name er python=3.10
conda activate er
pip3 install -r requirements.txt
```

### Start the service

```commandline
python3 ./make_data.py
python3 ./pre_compute.py
python3 ./main.py 
```
### Demo how to run

Watch Demo.mp4
