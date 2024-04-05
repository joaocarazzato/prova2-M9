values = []

with open("data_values.txt") as file:
    while line := file.readline():
        # print(line.rstrip())
        values.append(line.rstrip())

print(values)
print(len(values))