lst = [1,0.0,2,0,4,6]




for item in lst:

    if item ==0:
        lst.remove(item)
        lst.append(item)

print(lst)


index = lst.index(0, lst.index(0) + 1)
display(index)
removed_element = lst.pop(index)

lst.append(removed_element)

print(lst)
