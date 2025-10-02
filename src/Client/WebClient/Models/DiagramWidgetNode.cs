using Blazor.Diagrams.Core.Geometry;
using Blazor.Diagrams.Core.Models;

namespace WebClient.Models
{
    public class DiagramWidgetNode : NodeModel
    {
        public DiagramWidgetNode(Point? position = null) : base(position) { }

        public string Name { get; set; } = "";
        public string Content { get; set; } = "";
    }
}
