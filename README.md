# Distributed-Systems

## lab1

### Part I: Map/Reduce input and output

The code we give you is missing two crucial pieces: the function that divides up the output of a map task, and the function that gathers all the inputs for a reduce task. These tasks are carried out by the doMap() function in common_map.go, and the doReduce() function in common_reduce.go respectively.

#### lint

```
go test -run Sequential
```

If the output did not show ok next to the tests, your implementation has a bug in it. To give more verbose output, set `debugEnabled = true` in `common.go`, and add `-v` to the test command above. You will get much more output along the lines of:

```
go test -v -run Sequential
```

