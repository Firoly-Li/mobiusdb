

### 想做的功能
- [ ] 以arrow为底座,arrow flight为主要通信方式(可以添加具体其他业务接口)
  - [ ] hankshand （*HandshakeRequest*）
  - [ ] list_flights
  - [ ] get_flight_info
  - [ ] poll_flight_info
  - [ ] get_schema
  - [ ] do_get
  - [ ] do_put
  - [ ] do_exchange （暂不实现）
  - [ ] do_action（*Action*）（暂不实现）
  - [ ] list_actions （暂不实现）

- [ ] 拥有实时数据库和历史数据库
  - [ ] rtdb 实时数据库
  - [ ] htdb 历史数据库

- [ ] 拥有实时数据的订阅、订阅功能

  - [ ] 可以通过websocket获取订阅的实时数据（具体接口还没考虑清楚）

    ```shell
    ws://xxx:8093/subscribe
    ```

  - [ ] 可以通过webhook去分发订阅的数据（通过do_active方法）

- [ ] 拥有历史数据的查询功能

  - [ ] 可查询时间段内的所有数据

    ```sql
    select * from 'table_name' where time > xxx and time < xxx ;
    ```

  - [ ] 可查询时间段内的归档数据(前提是归档要提前设置为任务)

    ```sql
    select * from 'archiving_table_name'  
    ```

- [ ] 可以设置任务:
  - [ ] 值计算（加减乘除）
  - [ ] 业务计算：
    - [ ] 当某个值 == x x 的时候，执行xxx操作



---

### Arrow Flight (version=1)

#### 1、BasicAuth

- username：账号

- password：密码

#### 2、FlightDescriptor

- Path：路径
- Cmd：命令
  - 
- Unknown：位置类型

#### 3、Ticket

#### 4、Action

















