# Cloud Sync

O cliente compartilhado está em `../photocraft_cloudsync/cloud-sync`, ao lado deste checkout. Os dois repositórios precisam conservar esses nomes e a posição de irmãos para compilar esta integração provisória. Não há cópia independente do cliente neste repositório. Essa restrição é de compilação; o executável distribuído não precisa dos checkouts para funcionar.

Veja `../../../photocraft_cloudsync/docs/cloud-sync` a partir deste diretório para o estado, decisões e testes; configuração da VPS: `../photocraft_cloudsync/cloud-sync/deploy/README.md` a partir da raiz deste checkout.

O adaptador nativo está em `crates/ui-egui/src/cloud_sync.rs`. A versão web não recebe o cliente nativo. A senha pode ser guardada no cofre seguro do sistema; sem cofre disponível, permanece somente na sessão. Nunca é gravada nas preferências.

Guia de clone, build e conexão local: [PhotoCraft Cloud Sync](https://github.com/madebyfelipe/photocraft_cloudsync/blob/main/cloud-sync/README.md).

## Tela inicial estilo Figma (HOME-01)

Implementada em 2026-10-15 (fase B2): biblioteca inicial (`crates/ui-egui/src/cloud_home.rs`, `cloud_home_ui.rs`, `cloud_preview.rs`) com todos os projetos da conta, cards com prévia, nome, última edição e estado da sincronização, busca, ordenação, filtros, recentes e criação, abertura, renomeação e "Salvar uma cópia…" diretamente pela biblioteca. O salvamento automático continua sem arquivos manuais no PC; o limite do cache local (512 MiB a 10 GiB, padrão 2 GiB) fica em **Conta**. Veja [`../cloud-sync.md`](../cloud-sync.md). Escopo e critérios de aceite compartilhados: [HOME-01](https://github.com/madebyfelipe/photocraft_cloudsync/blob/main/docs/cloud-sync/TASKS.md#home-01--tela-inicial-de-projetos-na-nuvem). Pendente: validação de ponta a ponta com dois computadores e Windows/macOS.
