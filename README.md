# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正。

质控品批次可被同一检测项目的多台仪器共用。当某台仪器出现连续偏差时，`freeze_order`（冻结单）把质控品批次、质控结果、仪器和患者结果批次接成一条隔离与恢复链路。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、共享批号关系、工作项和审计查询。
- `src/service.py`：用例编排、冻结隔离/恢复、断点续做、旧数据升级和权限校验。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次，`freeze_order`为冻结单。

## 隔离与恢复流程

1. **开单（按原因去重）**：`POST /api/freeze_orders`，传`trigger_qc_run_id`和`reason`。原因键默认为`qc_run:<run_id>`，也可用`cause_key`显式指定；同一原因只有一份冻结单，重复提交返回409并带回既有冻结单。
2. **按批号隔离**：冻结单依据批号-仪器共享关系（`lot_instruments`）找出受影响仪器，逐台置`blocked`；该批号下待放行批次（`waiting`/已被其他冻结拦截的`intercepted`）逐批复审拦截。每一项都是独立工作项，成功一项提交一项并写一条审计。
3. **已放行批次不动账**：已放行批次保持原状态、版本和放行依据不变，冻结单只保存证据快照（`released_batch_snapshots`）。
4. **复核通过前不能放行**：冻结状态为`freezing/frozen/reviewed/review_rejected/recovering`时，相关批次放行返回409并附`details`（冻结单、仪器、当前阻塞批次、复核人）。
5. **复核**：`review`动作，`decision=approved|rejected`，驳回必须带`comment`，开单人不能复核自己的单。两名审核员同时提交时乐观锁只放行一个，后到者收到409和最新阻塞项。
6. **恢复**：`recover`按原工作项反向解除仪器封锁和批次拦截；一张批次被多份冻结覆盖时，只有全部冻结恢复才回到`waiting`。
7. **写入失败续做**：`POST /api/entities/<id>/actions`，`action=resume`（或`recover`）。未完成工作项保留为`pending`，重试只续做剩余项，已完成项不重复执行、不重复审计。
8. **旧数据升级**：`POST /api/maintenance/upgrade_legacy_lot_links`。旧质控没有共享批号关系时，按每条质控结果所属的单台仪器补全为`legacy`关系；旧关系不会扩大冻结范围，从旧质控结果触发的冻结只隔离该台仪器。升级可重复执行（幂等）。

冻结单状态：`freezing → frozen → reviewed → recovering → recovered`，复核驳回为`review_rejected`（可再次复核）。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/maintenance/upgrade_legacy_lot_links`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。409响应的`details`字段携带最新阻塞项。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。冻结流程演示的是按批号扩散、证据保留、乐观并发和断点续做的工程机制，不构成医疗合规结论。
