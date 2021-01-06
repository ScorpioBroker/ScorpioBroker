************
システム要件
************

Java 8 のシステム要件
#####################

**Windows**

- Windows 10 (8u51 以降)
- Windows 8.x (デスクトップ)
- Windows 7 SP1
- Windows Vista SP2
- Windows Server 2008 R2 SP1 (64-bit)
- Windows Server 2012 and 2012 R2 (64-bit)
- RAM: 128 MB
- ディスク容量: 124 MB for JRE; 2 MB for Java Update
- プロセッサー: 最小 Pentium 2 266 MHz processor
- ブラウザ: Internet Explorer 9 以降, Firefox

**Mac OS X**

- Mac running Mac OS X 10.8.3+, 10.9+ を実行しているインテルベース Mac
- インストールの管理者権限
- 64ビットブラウザ
- Mac で Oracle Java を実行するには、64ビットブラウザ (Safari など) が必要です

**Linux**

- Oracle Linux 5.5+1
- Oracle Linux 6.x (32-bit), 6.x (64-bit)2
- Oracle Linux 7.x (64-bit)2 (8u20 以降)
- Red Hat Enterprise Linux 5.5+1, 6.x (32-bit), 6.x (64-bit)2
- Red Hat Enterprise Linux 7.x (64-bit)2 (8u20 以降)
- Suse Linux Enterprise Server 10 SP2+, 11.x
- Suse Linux Enterprise Server 12.x (64-bit)2 (8u31 以降)
- Ubuntu Linux 12.04 LTS, 13.x
- Ubuntu Linux 14.x (8u25 以降)
- Ubuntu Linux 15.04 (8u45 以降)
- Ubuntu Linux 15.10 (8u65 以降)
- ブラウザ: Firefox


ZooKeeper の要件
################

ZooKeeper は Java リリース 1.6 以降 (JDK 6 以降 ) で実行されます。これは、ZooKeeper サーバーのアンサンブルとして
実行されます。3台の ZooKeeper サーバーは、アンサンブルの最小推奨サイズであり、別々のマシンで実行することもお勧めします。
Yahoo! では、ZooKeeper は通常、デュアルコアプロセッサ、2GB の RAM、および80GB の IDE ハードドライブを備えた専用の
RHEL ボックスにデプロイされます。

Kafka について提言
##################

**Kafka brokers** は、JVM ヒープと OS ページキャッシュの両方を使用します。JVM ヒープは、ブローカー間のパーティションの
レプリケーションとログの圧縮に使用されます。レプリケーションには、ブローカーのパーティションごとに1MB (デフォルトの
replica.max.fetch.size) が必要です。Apache Kafka 0.10.1 (Confluent Platform 3.1) では、レプリケーションに使用される
RAM の合計を10MBに制限する新しい構成 (replica.fetch.response.max.bytes) を追加して、メモリとガベージコレクションの問題を
回避します。ブローカーのパーティションは高いです。ログの圧縮の場合、必要なメモリの計算はより複雑です。この機能を使用
している場合は、Kafka のドキュメントを参照することをお勧めします。小規模から中規模の展開の場合、通常は4GBのヒープサイズ
で十分です。さらに、消費者は常にメモリから読み取ることを強くお勧めします。さらに、Consumer は常にメモリから読み取る
ことを強くお勧めします。つまり、Kafka に書き込まれ、OS ページキャッシュに保存されているデータから読み取ることを強く
お勧めします。これに必要なメモリの量は、このデータが書き込まれる速度と、消費者がどれだけ遅れると予想されるかによって
異なります。ブローカーごとに1時間あたり20GBを書き込み、通常のシナリオでブローカーが3時間遅れることを許可する場合は、
OS ページキャッシュに60GBを予約する必要があります。Consumer がディスクからの読み取りを強制される場合、パフォーマンスは
大幅に低下します

**Kafka Connect** それ自体は多くのメモリを使用しませんが、一部のコネクタは効率のためにデータを内部でバッファリング
します。バッファリングを使用する複数のコネクタを実行する場合は、JVM ヒープサイズを1GB以上に増やす必要があります。

**Consumers** は、Consumer ごとに少なくとも2MBを使用し、ブローカーからの大きな応答の場合は最大64MBを使用します (通常、
バーストトラフィックの場合)。プロデューサーはそれぞれ64MBのバッファーを持ちます。 まず、1GBのRAMを割り当て、Producer
ごとに64MB、Consumer ごとに16MBを追加します。
