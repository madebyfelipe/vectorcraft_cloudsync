# Cloud Sync

O cliente compartilhado está em `../photocraft_cloudsync/cloud-sync`, ao lado deste checkout. Os dois repositórios precisam conservar esses nomes e a posição de irmãos para compilar esta integração provisória. Não há cópia independente do cliente neste repositório. Essa restrição é de compilação; o executável distribuído não precisa dos checkouts para funcionar.

Veja `../../../photocraft_cloudsync/docs/cloud-sync` a partir deste diretório para o estado, decisões e testes; configuração da VPS: `../photocraft_cloudsync/cloud-sync/deploy/README.md` a partir da raiz deste checkout.

O adaptador nativo está em `crates/ui-egui/src/cloud_sync.rs`. A versão web não recebe o cliente nativo. A senha pode ser guardada no cofre seguro do sistema; sem cofre disponível, permanece somente na sessão. Nunca é gravada nas preferências.

Guia de clone, build e conexão local: [PhotoCraft Cloud Sync](../../../photocraft_cloudsync/cloud-sync/README.md).

## TODO — tela inicial estilo Figma (HOME-01)

Solicitado em 2026-10-15, pendente de implementação: biblioteca inicial com todos os projetos/posts da conta, cards e prévias, busca, recentes e criação/abertura diretamente pela nuvem. Salvamento automático sem exigir arquivos manuais no PC; downloads sob demanda e cache local limitado, preservando edições offline, uploads pendentes e conflitos. Exportação/cópia local por ação explícita. Escopo e critérios de aceite compartilhados: [HOME-01](../../../photocraft_cloudsync/docs/cloud-sync/TASKS.md#home-01--tela-inicial-de-projetos-na-nuvem). Não incluído no release Windows atual.
