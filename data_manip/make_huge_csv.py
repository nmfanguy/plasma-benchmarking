line = "{0},0000-1111-2222-3333,2021-10-16,Johhny Appleseed,26.0\n"

with open("in/huge_test_data.csv", "a") as f:
    f.write("id,uuid,date-edited,name,value")
    for i in range(1000000):
        f.write(line.format(i))
