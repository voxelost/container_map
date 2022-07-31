# Overwiew
A map[Hashable]Picklable implementation making use of docker containers. It features automatic container creation and garbage-collection, immutability of the stored data and pretty fast operations (an average of 0.12s for each insertion and an average of 0.18s for each deletion for alpine linux Docker containers running under podman VM on my machine).

# Why
why not

# Running the demo
You're going to need a running Docker daemon locally or set the `DOCKER_HOST` env variable to one of your choice.

```
python3 -m pip install pipenv
python3 -m pipenv install --deploy
python3 main.py
```
