# Module `lakehouse_storage`

Wrapper quanh `Azure/avm-res-storage-storageaccount/azurerm` **0.11.0**, viết ở bước 3.2 của stack `10-core`
([docs/devops/03](../../../docs/devops/03_terraform_azure.md)).

Module tạo:
- Storage account ADLS Gen2: `StorageV2`, `Standard_LRS`, bật HNS, tắt shared key, chỉ xác thực bằng Entra ID.
- 7 container theo tầng: `landing`, `bronze`, `silver`, `gold`, `checkpoints`, `mlflow`, `ds-data`.
  Container của DS tên `ds-data` vì Azure bắt tên container dài ít nhất 3 ký tự.
- Lifecycle rule `landing-to-cool`: blob dưới `landing/` chuyển sang tier cool sau 30 ngày kể từ lần sửa cuối.

Lưu ý:
- AVM 0.11.0 chỉ dùng provider `azapi` (`~> 2.12`) và cần Terraform `>= 1.10`.
- Mặc định của AVM là tắt public network và chặn mọi IP. Module mở lại public network vì dự án không dùng
  private endpoint hay VNet injection. Quyền truy cập được kiểm bằng RBAC.
- Quyền đọc ghi dữ liệu không cấp ở đây. Access Connector được cấp ở bước 3.4, ADF ở bước 3.7, thành viên ở bước 3.8.
