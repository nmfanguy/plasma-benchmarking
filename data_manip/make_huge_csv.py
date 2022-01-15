line = "{0},0000-1111-2222-3333,Johhny Appleseed,26.0\n"

with open("in/huge_test_data.csv", "a") as f:
    f.write("id,uuid,name,value\n")
    for i in range(1000000):
        f.write(line.format(i))
