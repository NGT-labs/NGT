ngtpy
=====

NGT python

Class Index
===========

## Member Functions

### \_\_init\_\_
指定されたインデックスをオープンし、そのインデックスのオブジェクトを生成します。

      __init__(self: ngtpy.Index, path: str, read_only: bool=False, zero_based_numbering: bool=True, log_disabled: bool=False)

**Returns**  
なし

**path**   
オープンするインデックスのパスを指定します。

**read_only**   
書込み不可でインデックスをオープンします。Falseは書込み可を意味します。

**zero_based_numbering**   
オブジェクトIDが０から始まることを指定します。Falseは１から始まることを意味します。

**log_disabled**    
処理の進捗に関する標準エラーのメッセージを無効にします。

### close
インデックスをクローズします。

      close(self: ngtpy.Index)

**Returns**   
なし

### insert
指定されたオブジェクトをインデックスに登録します。この関数はオブジェクトの検索グラフを生成しません。登録後に `build_index` を呼び出してください。`build_index` は、まだグラフノードを持たないオブジェクトだけを構築します。

      int insert(self: ngtpy.Index, object: numpy.ndarray[float64])

**Returns**   
登録したオブジェクトのID

**object**   
登録するオブジェクトを指定します。

### build_index
関数 `insert` で登録されたオブジェクトのうち、まだグラフノードを持たないオブジェクトの検索グラフを生成します。既にグラフノードを持つオブジェクトはスキップされます。内部的には未構築のオブジェクトを探すためにオブジェクト一覧を走査しますが、既存のグラフノードを作り直すことはありません。

グラフ検索（`search`）は、構築済みのオブジェクトしか返せません。新しく登録したオブジェクトは、`build_index` を呼ぶまで `search` では返されません。`linear_search` は未構築のオブジェクトも含めて全登録オブジェクトを走査します。

      build_index(self: ngtpy.Index, num_threads: int=8)

**Returns**   
なし

**num_threads**  
インデックスを生成する時に利用するスレッド数を指定します。

### batch_insert
指定した複数のオブジェクトを登録し、そのオブジェクトのインデックスを生成します。この関数はngtコマンドの"ngt append"を実行するのとほぼ同じです。この関数を呼び出す代わりにngtコマンドを使っても構いません。インデックスは `build=True`（デフォルト）のときだけ生成されます。

      batch_insert(self: ngtpy.Index, objects: numpy.ndarray[float64], num_threads: int=8)

**Returns**  
なし

**objects**   
登録する複数のオブジェクトを指定します。

**num_thread**   
インデックスを生成する時に利用するスレッド数を指定します。


### remove
指定されたオブジェクトを削除します。

      remove(self: ngtpy.Index, object_id: int)

**Returns**  
なし

**object_id**   
削除するオブジェクトのIDを指定します。

### save
インデックスを保存します。

      save(self: ngtpy.Index)

**Returns**  
なし

### get_object
指定されたオブジェクトを取得します。

      List[float] get_object(self: ngtpy.Index, object_id: int)

**Returns**   
指定されたオブジェクト

### get_num_of_objects
登録済みのオブジェクト数を返します。

      int get_num_of_objects()

**Returns**   
登録済みのオブジェクト数

### search
指定されたクエリオブジェクトに対する近傍のオブジェクトを検索します。

      object search(self: ngtpy.Index, query: object, size: int, epsilon: float, edge_size: int, with_distance: bool=True)

**Returns**   
検索結果としてタプル（ID、距離）のリスト

**query**   
クエリオブジェクトを指定します。

**size**   
検索結果として返るオブジェクトの数を指定します。

**epsilon**   
グラフの探索範囲を決定する変数イプシロンを指定します。

**edge_size**   
グラフを探索するのに利用する各ノードのエッジ数を指定します。

**with_distance**   
距離付きオブジェクトIDの検索結果を返すことを指定します。Falseは結果がオブジェクトIDののみのリストとなることを意味します。

### set
検索パラメータのデフォルト値を指定します。

      set(self: ngtpy.Index, num_of_search_objects: int, epsilon: float, search_radius: float)

**Returns**   
なし。

**num_of_search_objects**    
検索結果数を指定します。初期デフォルト値は20です。

**epsilon**   
グラフの探索範囲を決定する変数イプシロンを指定します。初期デフォルト値は0.1です。

**search_radius**    
検索範囲を指定します。初期デフォルト値は無限です。

### export_index
インデックスをエクスポートします。

      export_index(self: ngtpy.Index, path: str)

**Returns**   
なし。

**path**    
エクスポートで保存されるパスを指定します。

### import_index
インデックスをインポートします。

      import_index(self: ngtpy.Index, path: str)

**Returns**   
なし。

**path**    
インポートするパスを指定します。

FUNCTIONS
=========

### create
空のインデックスを生成します。この関数はngtコマンドの"ngt create"を実行するのとほぼ同じです。この関数を呼び出す代わりにngtコマンドを使っても構いません。

      create(path: str, dimension: int, edge_size_for_creation: int=10, edge_size_for_search: int=40, distance_type: str='L2', object_type: str='Float')

**Returns**   
なし

**path**   
インデックスのパスを指定します。

**dimension**  
登録するオブジェクトの次元数を指定します。

**edge_size_for_creation**   
各ノードの初期エッジ数を指定します。

**edge_size_for_search**   
検索時にグラフを探索するためのノードのエッジ数を指定します。

**distance_type**   
オブジェクトの距離関数を指定します。
- __L1__: L1 距離
- __L2__: L2 距離（デフォルト）
- __Angle__: 角度距離
- __Normalized Angle__: 正規化角度距離。指定されたデータは自動的に正規化された上でインデックスに登録されます。
- __Cosine__: コサイン類似度
- __Normalized Cosine__: 正規化コサイン類似度。指定されたデータは自動的に正規化された上でインデックスに登録されます。
- __Hamming__: ハミング距離
- __Jaccard__: ジャッカード距離
- __Inner Product__: 内積

**object_type**  
オブジェクトのデータタイプを指定します。
- __Float__: 4 バイト浮動小数点
- __Float16__: 2 バイト浮動小数点
- __Byte__: 1 バイト符号なし整数

Class Optimizer
===============

### \_\_init\_\_
指定されたパラメータを設定したoptimizerオブジェクトを生成します。

      __init__(self: ngtpy.Optimizer, num_of_outgoings: int=10, num_of_incomings: int=120, log_disabled: bool=False)

**Returns**  
なし

**num_of_outgoings**    
入力のグラフから再構築のグラフへ加える各ノードの出力エッジ数を指定します。指定された値は再構築グラフの出次数の下限値を意味します。

**num_of_incomings**    
入力のグラフから再構築のグラフへ加える各ノードの入力エッジ数を指定します。*num_of_outgoings*とは異り、エッジの方向を逆転した後、再構築のグラフに加ます。この値は再構築グラフの入次数の下限値を意味します。

**log_disabled**    
処理の進捗に関する標準エラーのメッセージを無効にします。

### execute
事前に指定されたパラメータを用いて指定されたインデックスから新なインデックスを再構築し、検索時の係数を最適化します。この最適化は*adjust_search_coefficients*を呼び出すのと同じです。

      execute(self: ngtpy.Optimizer, in_index_path: str, out_index_path: str)


**in_index_path**    
入力のインデックスを指定します。


**out_index_path**    
出力のインデックスを指定します。

### adjust_search_coefficients
検索係数を最適化します。

      adjust_search_coefficients(self: ngtpy.Optimizer, index_path: str)

**index_path**    
最適化するインデックスを指定します。

Class QuantizedIndex
===========

## Member Functions

### \_\_init\_\_
指定された量子化インデックスをオープンし、そのインデックスのオブジェクトを生成します。

      __init__(self: ngtpy.QuantizedIndex, path: str, zero_based_numbering: bool=True, log_disabled: bool=False)

**Returns**  
なし

**path**   
オープンするインデックスのパスを指定します。量子化インデックスは`qbg create-qg`と`qbg build-qg`で事前にONNGやANNGからビルドしてください。pythonでの量子化する関数はまだ利用できません。

**zero_based_numbering**   
オブジェクトIDが０から始まることを指定します。Falseは１から始まることを意味します。

**log_disabled**    
処理の進捗に関する標準エラーのメッセージを無効にします。

### search
指定されたクエリオブジェクトに対する近傍のオブジェクトを検索します。

      object search(self: ngtpy.QuantizedIndex, query: object, size: int=20, epsilon: float, result_expansion: float)

**Returns**   
検索結果としてタプル（ID、距離）のリスト

**query**   
クエリオブジェクトを指定します。

**size**   
検索結果として返るオブジェクトの数を指定します。

**epsilon**   
グラフの探索範囲を決定する変数イプシロンを指定します。

**result_expansion**   
検索結果数に対する内部の近似検索結果数の拡張割合を指定します。例えば、この割合が10で検索結果数が20の場合には、検索処理中の近似検索数は200に設定されます。大きな値ほど精度は高くなりますが、検索に時間がかかります。

### set
検索パラメータのデフォルト値を指定します。

      set(self: ngtpy.QuantizedIndex, num_of_search_objects: int, epsilon: float, result_expansion: float)

**Returns**   
なし。

**num_of_search_objects**    
検索結果数を指定します。初期デフォルト値は20です。

**epsilon**   
グラフの探索範囲を決定する変数イプシロンを指定します。初期デフォルト値は0.02です。

**result_expansion**   
検索結果数に対する内部の近似検索結果数の拡張割合を指定します。初期デフォルト値は3.0です。
