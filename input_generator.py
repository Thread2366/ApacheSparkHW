from datetime import datetime, timedelta
from random import randint

def generate_samples():
    now = datetime.now()
    return ((str(randint(0, 10)), str(i), str(randint(0, 100))) 
            for i in range(
                int(now.timestamp()), 
                int((now + timedelta(1)).timestamp())))

samples = generate_samples()

with open('input.csv', 'w') as file:
    for s in samples:
        file.write(', '.join(s) + '\n')
