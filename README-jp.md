<div align="center">
<img src="./assets/logo.svg" width="50%">
</div>

Neighborhood Graph and Tree for Indexing High-dimensional Data

[トップ](/README-jp.md) / [インストール](/README-jp.md#インストール) / [コマンド](/bin/ngt/README-jp.md) / [ライセンス](/README-jp.md#ライセンス) / [関連文献](/README-jp.md#関連文献) / [About Us](http://research-lab.yahoo.co.jp/) / [English](/README.md)

大量（数百万から数千万）の高次元ベクトルデータ（数十～数千次元）に対して高速な近似近傍検索を可能とするコマンド及びライブラリを提供します。

ニュース
-------
- 05/26/2025 直積量子化がグラフのデータタイプとして利用可能になりました。 (v2.4.0)
- 10/29/2024 スカラ量子化がグラフやQBGのデータタイプとして利用可能になりました。(v2.3.0)
- 2024/04/10 内積が利用可能になりました。(v2.1.0)
- 2022/08/10 QBG(Quantized Blob Graph)およびQG(NGTQGの改良版)が利用可能となりました。ngtqおよびngtqgは[qbg](https://github.com/yahoojapan/NGT/blob/main/bin/qbg/README.md)で置き換えられました。
- 2022/02/04 FP16(半精度浮動小数点)が利用可能になりました。(v1.14.0)
- 2021/03/12 READMEに量子化グラフの結果を追加しました。
- 2021/01/15 [量子化グラフ (NGTQG)](bin/ngtqg/README.md)を実装した NGT v1.13.0 をリリースしました。
- 2019/11/04 [NGT チュートリアル](https://github.com/yahoojapan/NGT/wiki) をリリースしました。
- 2019/06/26 Jaccard距離が利用可能になりました。(v1.7.6)
- 2019/06/10 PyPI NGT パッケージ v1.7.5 が利用可能になりました。
- 2019/01/17 Python NGTはPYPIからpipでインストールが可能になりました。(v1.5.1)
- 2018/12/14 [NGTQ](bin/ngtq/README-jp.md) (NGT with Quantization) が利用可能になりました。(v1.5.0)
- 2018/08/08 [ONNG](README-jp.md#onng)が利用可能になりました。(v1.4.0)

手法
---
このリポジトリは次の手法を提供します。
- NGT: Graph and tree-based method
- QG: Quantized graph-based method
- QBG: Quantized blob graph-based method

注：QGおよびQBGはBLASおよびLAPACKライブラリを必要としますので、もし、V1のようにNGT (Graph and tree-based method)のみしか利用しない場合には、QGおよびQBGを[このオプション](#QGおよびQBGの無効化)により無効化できます。

インストール
-----------

### ダウンロード

- [Releases](https://github.com/yahoojapan/NGT/releases)

### ビルド

#### Linux （QG QBGの無効化）

      $ unzip NGT-x.x.x.zip
      $ cd NGT-x.x.x
      $ mkdir build
      $ cd build
      $ cmake -DNGT_QBG_DISABLED=ON ..
      $ make
      $ make install
      $ ldconfig /usr/local/lib

#### CentOS

      $ yum install blas-devel lapack-devel
      $ unzip NGT-x.x.x.zip
      $ cd NGT-x.x.x
      $ mkdir build
      $ cd build
      $ cmake ..
      $ make
      $ make install
      $ ldconfig /usr/local/lib

#### Ubuntu

      $ apt install libblas-dev liblapack-dev
      $ unzip NGT-x.x.x.zip
      $ cd NGT-x.x.x
      $ mkdir build
      $ cd build
      $ cmake ..
      $ make
      $ make install
      $ ldconfig /usr/local/lib

#### macOS

      $ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
      $ brew install cmake
      $ brew install libomp
      $ unzip NGT-x.x.x.zip
      $ cd NGT-x.x.x
      $ mkdir build
      $ cd build
      $ export OpenMP_ROOT=$(brew --prefix)/opt/libomp
      $ cmake ..
      $ make
      $ make install

### ビルド済み

#### macOS

      $ brew install ngt

NGT (Graph and tree-based method)
=================================

特徴
----
- OS：Linux、macOS
- データの追加削除が可能
- [共有メモリ（マップドメモリ）](README-jp.md#共有メモリの利用)のオプションによるNGTではメモリサイズを超えるデータが利用可能
- データ型：1バイト整数、4バイト単精度浮動小数点、2バイト半精度浮動小数点
- 距離関数：L1、L2、コサイン類似度、角度、ハミング、ジャッカード、ポアンカレ、ローレンツ、内積
- 対応言語：[Python](/python/README-jp.md)、[Ruby](https://github.com/ankane/ngt)、[PHP](https://github.com/ankane/ngt-php)、[Rust](https://crates.io/crates/ngt)、[Go](https://github.com/yahoojapan/gongt)、C、C++
- 分散サーバ：[ngtd](https://github.com/yahoojapan/ngtd), [vald](https://github.com/vdaas/vald)

ドキュメント
-----------

- [NGT チュートリアル](https://github.com/yahoojapan/NGT/wiki)


ユーティリティ
-------------

- コマンド : [ngt](/bin/ngt/README-jp.md#command)
- サーバ : [ngtd](https://github.com/yahoojapan/ngtd), [vald](https://github.com/vdaas/vald)

対応言語
--------

- [Python](/python/README-jp.md)
- [Ruby](https://github.com/ankane/ngt) (Thanks Andrew!)
- [PHP](https://github.com/ankane/ngt-php) (Thanks Andrew!)
- [Rust](https://crates.io/crates/ngt) (Thanks Romain!)
- JavaScript/NodeJS : [ngt-tool](https://www.npmjs.com/package/ngt-tool), [spatial-db-ngt](https://www.npmjs.com/package/spatial-db-ngt) (Thanks stonkpunk!)
- [Go](https://github.com/yahoojapan/gongt)
- C
- C++([sample code](samples))

ビルドパラメータ
-----------

#### 共有メモリの利用

メモリマップドファイルを用いた共有メモリにインデックスを配置することが可能です。共有メモリを利用することにより複数のプロセスが同一のインデックスを利用する場合にメモリ使用量を抑制することが可能です。さらに、メモリにロードできないような大量のオブジェクトを有するインデックスを扱うことが可能なだけでなく、インデックスをオープンする時間を削減することも可能です。共有メモリを利用するにはビルド時の変更が必要となりますので、cmake実行時に以下のパラメータを追加してください。

      $ cmake -DNGT_SHARED_MEMORY_ALLOCATOR=ON ..

注：ロック機能はありませんので、複数プロセスで同一のインデックスを利用する場合には参照のみでご使用ください。

#### 大規模データの利用

約500万以上のオブジェクトをNGTに登録する場合には、検索速度向上のために以下のパラメータを追加してください。

      $ cmake -DNGT_LARGE_DATASET=ON ..

#### QGおよびQBGの無効化

QGおよびQBGはBLASおよびLAPACKライブラリを必要とします。もし、これらのライブラリをインストールしたくなく、かつ、QGやQBGを利用しない場合には、QGおよびQBGを無効化できます。

      $ cmake -DNGT_QBG_DISABLED=ON ..

QG (Quantized graph-based method)
=================================

特徴
----
- NGTよりも高性能
- OS：Linux、macOS
- 距離関数：L2、コサイン類似度
- 対応言語：C++, C, Python

ドキュメント
-----------
- [QG チュートリアル](https://techblog.yahoo.co.jp/entry/2023031330416837/)

ユーティリティ
-------------

- コマンド : [qbg](bin/qbg/README.md)

対応言語
--------

- C++
- C
- Python （検索のみ対応）

ビルドパラメータ
-----------

QGでは性能向上のためにベクトル空間の回転や残差ベクトルを無効化することを推奨しています。

      $ cmake -DNGTQG_NO_ROTATION=ON -DNGTQG_ZERO_GLOBAL=ON ..

QBG (Quantized blob graph-based method)
=======================================

特徴
----
- 10億ものオブジェクトの検索が可能
- OS：Linux、macOS
- 距離関数：L2
- 対応言語：C++, C, Python

ドキュメント
-----------
- [QBG チュートリアル](https://medium.com/@masajiro.iwasaki/qbg-in-ngt-an-overview-and-first-steps-d9c33f6168e2)

ユーティリティ
-------------
- コマンド : [qbg](bin/qbg/README.md)

対応言語
--------

- C++
- C
- Python （検索のみ対応）

ベンチマーク結果
---------------

以下の結果は NGT v2.8.0 のベンチマーク結果であり、[さくらインターネット株式会社](https://www.sakura.ad.jp/corporate/)が提供する [さくらのクラウド](https://cloud.sakura.ad.jp/) 環境上の Intel(R) Xeon(R) Gold 6230R を用いて測定しています。

#### glove-100-angular
<img src="./tests/ann-benchmarks-results/glove-100-angular.png?raw=true" width="600">


ライセンス
----------

Copyright (C) 2015 Yahoo Japan Corporation  
Copyright (C) 2026 SAKURA internet Inc.

本ソフトウェアは Apache License Version 2.0（以下「ライセンス」）の下で提供されています。ライセンスに準拠する場合を除き、本ソフトウェアを使用することはできません。ライセンスのコピーは以下から入手できます。

    https://www.apache.org/licenses/LICENSE-2.0

本ソフトウェアで使用されている技術に関する特許権者は、本ソフトウェアが Apache License Version 2.0 に従って利用される限り、当該特許権を利用者に対して行使しません。

コントリビューション
--------------------

プルリクエストを送信する前に、[CONTRIBUTING.md](CONTRIBUTING.md) をご確認ください。

お問い合わせ
------------

[masajiro](https://github.com/masajiro)

関連文献
--------
##### [ONNG](bin/ngt/README-jp.md#onng)
- Iwasaki, M., Miyazaki, D.: Optimization of Indexing Based on k-Nearest Neighbor Graph for Proximity. arXiv:1810.07355 [cs] (2018). ([pdf](https://arxiv.org/abs/1810.07355))

##### [PANNG](bin/ngt/README-jp.md#panng)
- Iwasaki, M.: Pruned Bi-directed K-nearest Neighbor Graph for Proximity Search. Proc. of SISAP2016 (2016) 20-33. ([pdf](https://link.springer.com/chapter/10.1007/978-3-319-46759-7_2))
- Sugawara, K., Kobayashi, H. and Iwasaki, M.: On Approximately Searching for Similar Word Embeddings. Proc. of ACL2016 (2016) 2265-2275. ([pdf](https://aclweb.org/anthology/P/P16/P16-1214.pdf))

##### [ANNGT](bin/ngt/README-jp.md#anngt)
- Iwasaki, M.: Applying a Graph-Structured Index to Product Image Search (in Japanese). IIEEJ Journal 42(5) (2013) 633-641. ([pdf](https://s.yimg.jp/i/docs/research_lab/articles/miwasaki-iieej-jnl-2013.pdf))
- Iwasaki, M.: Proximity search using approximate k nearest neighbor graph with a tree structured index (in Japanese). IPSJ Journal 52(2) (2011) 817-828. ([pdf](https://s.yimg.jp/i/docs/research_lab/articles/miwasaki-ipsj-jnl-2011.pdf))

##### [ANNG](bin/ngt/README-jp.md#anng)
- Iwasaki, M.: Proximity search in metric spaces using approximate k nearest neighbor graph (in Japanese). IPSJ Trans. on Database 3(1) (2010) 18-28. ([pdf](https://s.yimg.jp/i/docs/research_lab/articles/miwasaki-ipsj-tod-2010.pdf))


