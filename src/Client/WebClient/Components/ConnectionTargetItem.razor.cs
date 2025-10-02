using Microsoft.AspNetCore.Components;
using MudBlazor;
using WebClient.Models;

namespace WebClient.Components
{
    public partial class ConnectionTargetItem
    {
        [Parameter] public ConnectonTargetDto Info { get; set; } = new();
        [Parameter] public Action? OnDeleteAction { get; set; }

        private bool _expanded = false;
        private List<string> Types = ["PostgreSql", "ClickHouse"];

        private string _expandedBtnIcon => _expanded
            ? Icons.Material.Outlined.KeyboardArrowUp
            : Icons.Material.Outlined.KeyboardArrowDown;

        private void OnExpandCollapseClick()
        {
            _expanded = !_expanded;
        }

        private void OnDeleteBtnClick()
        {
            if (OnDeleteAction != null)
                OnDeleteAction.Invoke();
        }
    }
}
