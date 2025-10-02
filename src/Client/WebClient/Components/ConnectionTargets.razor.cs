using Microsoft.AspNetCore.Components;
using WebClient.Models;

namespace WebClient.Components
{
    public partial class ConnectionTargets
    {
        [Parameter] public List<ConnectonTargetDto> ConnectonTargetItems { get; set; } = [];

        private void OnClickCreateConnection()
        {
            ConnectonTargetItems.Add(new());
        }

        private void OnDeleteAction(Guid id)
        {
            var current = ConnectonTargetItems.FirstOrDefault(x => x.Id == id);
            if(current != null)
            ConnectonTargetItems.Remove(current);

            StateHasChanged();
        }
    }
}
