ngtpy
=====

NGT python

Class Index
===========

## Member Functions

### \_\_init\_\_
Open the specified index and create the index object for the index.

      __init__(self: ngtpy.Index, path: str, read_only: bool=False, zero_based_numbering: bool=True, log_disabled: bool=False)

**Returns**  
None.

**path**   
Specify the path of the index to open.

**read_only**   
Open the index as read-only. False means opening as read-write.

**zero_based_numbering**   
Specify zero-based numbering for object IDs. False means one-based numbering.

**log_disabled**    
Disable stderr messages about the progression of an operation.

### close
Close the index.

      close(self: ngtpy.Index)

**Returns**   
None.

### insert
Insert the specified object into the index. This function does not build the search graph for the object. Call `build_index` after inserting objects. `build_index` builds only objects that do not yet have graph nodes.

      int insert(self: ngtpy.Index, object: numpy.ndarray[float64])

**Returns**   
ID for the inserted object.

**object**   
Specify the inserted object.

### build_index
Build the search graph for objects that have been inserted with `insert` and do not yet have graph nodes. Objects that already have graph nodes are skipped. Internally the object list is scanned to find unbuilt objects, but existing graph nodes are not rebuilt from scratch.

The graph search (`search`) cannot return newly inserted objects until they have been built. `linear_search` scans all registered objects, including objects that have not been built yet.

      build_index(self: ngtpy.Index, num_threads: int=8)

**Returns**   
None.

**num_threads**  
Specify the number of threads used to build the index.

### batch_insert
Insert the specified objects and build the index for the objects. This function is almost the same as executing the ngt command "ngt append". You may execute the ngt command instead of calling this function. The index is built only when `build=True` (the default).

      batch_insert(self: ngtpy.Index, objects: numpy.ndarray[float64], num_threads: int=8)

**Returns**  
None.

**objects**   
Specify the inserted objects.

**num_thread**   
Specify the number of threads to build the index.


### remove
Remove the specified object.

      remove(self: ngtpy.Index, object_id: int)

**Returns**  
None.

**object_id**   
Specify the removed object ID.

### save
Save the index.

      save(self: ngtpy.Index)

**Returns**  
None.

### get_object
Get the specified object.

      List[float] get_object(self: ngtpy.Index, object_id: int)

**Returns**   
The specified object.


### get_num_of_objects
Get the number of the registered objects.

      int get_num_of_objects()

**Returns**   
The number of the registered objects.


### search
Search the nearest objects to the specified query object.

      object search(self: ngtpy.Index, query: object, size: int, epsilon: float, edge_size: int, with_distance: bool=True)

**Returns**   
The list of tuples(object ID, distance) as the search result. 

**query**   
Specify the query object.

**size**   
Specify the number of the objects as the search result.

**epsilon**   
Specify epsilon which defines the explored range for the graph.

**edge_size**   
Specify the number of edges for each node to explore the graph.

**with_distance**   
Specify object IDs with distances as the result. False means that the result is a list of only object IDs.


### set
Specify the default search parameters.

      set(self: ngtpy.Index, num_of_search_objects: int, epsilon: float, search_radius: float)

**Returns**   
None.

**num_of_search_objects**    
Specify the number of search objects. The initial default is 20.

**epsilon**   
Specify the epsilon which defines the explored range for the graph. The initial default is 0.1.

**search_radius**    
Specify the search radius. The initial default is infinity.

### export_index
Exports the index to a file.

      export_index(self: ngtpy.Index, path: str)

**Returns**   
None.

**path**    
Path to file in which the exported index will be stored.

### import_index
Imports the index from a file

      import_index(self: ngtpy.Index, path: str)

**Returns**   
None.

**path**    
Path to file from which to load the index.


FUNCTIONS
=========

### create
Create an empty index. This function is almost the same as executing the ngt command "ngt create" with an empty inserted object file. You may execute the ngt command instead of calling this function.

      create(path: str, dimension: int, edge_size_for_creation: int=10, edge_size_for_search: int=40, distance_type: str='L2', object_type: str='Float')


**Returns**   
None.

**path**   
Specify the path of the index.

**dimension**  
Specify the dimensionality of the inserted object.

**edge_size_for_creation**   
Specify the initial number of edges for each node.

**edge_size_for_search**   
Specify the number of edges for each node to explore the graph for the search processing.

**distance_type**   
Specify the distance function for the objects.
- __L1__: L1 distance
- __L2__: L2 distance (default)
- __Normalized L2__: Normalized L2 distance. The specified data are automatically normalized to be appended to the index.
- __Angle__: Angle distance
- __Normalized Angle__: Normalized angle distance. The specified data are automatically normalized to be appended to the index.
- __Cosine__: Cosine similarity
- __Normalized Cosine__: Normalized cosine similarity. The specified data are automatically normalized to be appended to the index.
- __Hamming__: Hamming distance
- __Jaccard__: Jaccard distance
- __Inner Product__: Inner product (or dot product)

**object_type**  
Specify the data type of the objects.
- __Float__: 4 byte floating point number
- __Float16__: 2 byte floating point number
- __Byte__: 1 byte unsigned integer

Class Optimizer
===============

### \_\_init\_\_
Create the optimizer object with the specified parameters.

      __init__(self: ngtpy.Optimizer, num_of_outgoings: int=10, num_of_incomings: int=120, log_disabled: bool=False)

**Returns**  
None.

**num_of_outgoings**    
Specify the number of outgoing edges for each node to add to the reconstructed graph from the input graph. The specified number also means the lower bound of the outdegrees of the reconstructed graph.

**num_of_incomings**    
Specify the number of incoming edges for each node to add to the reconstructed graph from the input graph. Unlike *num_of_outgoings*, after the direction of the edges are reversed, the edges are added to the reconstructed graph. The specified number also means the lower bound of the indegrees of the reconstructed graph.

**log_disabled**    
Disable stderr messages about the progression of an operation.

### execute
Reconstruct an index from the specified index with the previously specified parameters, and optimize search coefficients, which is the same as call *adjust_search_coefficients* below.


      execute(self: ngtpy.Optimizer, in_index_path: str, out_index_path: str)


**in_index_path**    
Specify the input index path.

**out_index_path**    
Specify the output index path.

### adjust_search_coefficients
Optimize search coefficients.

      adjust_search_coefficients(self: ngtpy.Optimizer, index_path: str)

**index_path**    
Specify the index which is optimized.

Class QuantizedIndex
===========

## Member Functions

### \_\_init\_\_
Open the specified quantized index and create the index object for the index.

      __init__(self: ngtpy.QuantizedIndex, path: str, zero_based_numbering: bool=True, log_disabled: bool=False)

**Returns**  
None.

**path**   
Specify the path of the quantized index to open. The quantized index should be built by using the command `ngtqg quantize` from ONNG or ANNG in advance. The python function for quantization is not available yet.

**zero_based_numbering**   
Specify zero-based numbering for object IDs. False means one-based numbering.

**log_disabled**    
Disable stderr messages about the progression of an operation.

### search
Search the nearest objects to the specified query object.

      object search(self: ngtpy.QuantizedIndex, query: object, size: int, epsilon: float, result_expansion: float)

**Returns**   
The list of tuples(object ID, distance) as the search result. 

**query**   
Specify the query object.

**size**   
Specify the number of the objects as the search result.

**epsilon**   
Specify epsilon which defines the explored range for the quantized graph.

**result_expansion**   
Specify the expansion ratio of the number of approximate inner search objects to the number of search objects. For example, when the ratio is 10 and the number of search objects is 20, the number of the approximate search objects is set to 200 inside the search processing. A larger value brings higher accuracy but slower searching.

### set
Specify the default search parameters.

      set(self: ngtpy.QuantizedIndex, num_of_search_objects: int, search_radius: float, result_expansion: float)

**Returns**   
None.

**num_of_search_objects**    
Specify the number of search objects. The initial default is 20.

**epsilon**   
Specify epsilon which defines the explored range for the graph. The initial default is 0.02.

**result_expansion**   
Specify the expansion ratio of the number of approximate inner search objects to the number of search objects. The initial default is 3.0.

