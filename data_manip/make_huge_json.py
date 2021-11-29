line = "{{\"id\": {0}, \"uuid\": \"0000-1111-2222-3333\", \"date-edited\": \"2021-10-16\", \"name\": \"Johhny Appleseed\", \"value\": 26.0}}\n"

with open("in/huge_test_data.json", "a") as f:
    for i in range(1000000):
        f.write(line.format(i))
