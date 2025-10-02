using Internal.DAL.Entities;
using Microsoft.EntityFrameworkCore;

namespace Internal.DAL;

public class PipelineDbContext : DbContext
{
    public PipelineDbContext()
    {
    }

    public PipelineDbContext(DbContextOptions<PipelineDbContext> options) : base(options)
    {
    }

    public DbSet<User> Users { get; set; } = null!;
    public DbSet<Pipeline> Pipelines { get; set; } = null!;
    public DbSet<PipelineDataNode> PipelineDataNodes { get; set; } = null!;
    public DbSet<FileNodeConfig> FileNodeConfigs { get; set; } = null!;
    public DbSet<DatabaseNodeConfig> DatabaseNodeConfigs { get; set; } = null!;
    public DbSet<PipelineFunction> PipelineFunctions { get; set; } = null!;
    public DbSet<PipelineRelationship> PipelineRelationships { get; set; } = null!;
    public DbSet<PipelineRun> PipelineRuns { get; set; } = null!;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Database=pipeline;Username=admin;Password=postgres;");
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.HasDefaultSchema("public");

        modelBuilder.Entity<User>(entity =>
        {
            entity.HasIndex(e => e.Email).IsUnique();
        });

        modelBuilder.Entity<Pipeline>(entity =>
        {
            entity.HasOne(p => p.CreatedUser)
                .WithMany()
                .HasForeignKey(p => p.CreatedUserId);

            entity.HasMany(p => p.DataNodes)
                .WithOne(n => n.Pipeline)
                .HasForeignKey(n => n.PipelineId);

            entity.HasMany(p => p.Functions)
                .WithOne(f => f.Pipeline)
                .HasForeignKey(f => f.PipelineId);

            entity.HasMany(p => p.Relationships)
                .WithOne(c => c.Pipeline)
                .HasForeignKey(c => c.PipelineId);

            entity.HasIndex(p => p.CreatedUserId);
            entity.HasIndex(p => p.CreatedAt);
        });

        modelBuilder.Entity<PipelineDataNode>(entity =>
        {
            entity.Property(n => n.NodeType)
                .HasConversion<string>();
            
            entity.Property(n => n.DataNodeType)
                .HasConversion<string>();

            entity.HasOne(n => n.FileConfig)
                .WithOne(f => f.DataNode)
                .HasForeignKey<FileNodeConfig>(f => f.DataNodeId);

            entity.HasOne(n => n.DatabaseConfig)
                .WithOne(d => d.DataNode)
                .HasForeignKey<DatabaseNodeConfig>(d => d.DataNodeId);
            
            entity.HasIndex(n => n.PipelineId);
            entity.HasIndex(n => new { n.PipelineId, n.NodeType });
        });

        modelBuilder.Entity<FileNodeConfig>(entity =>
        {
            entity.HasIndex(f => f.DataNodeId).IsUnique();
        });

        modelBuilder.Entity<DatabaseNodeConfig>(entity =>
        {
            entity.HasIndex(d => d.DataNodeId).IsUnique();
        });

        modelBuilder.Entity<PipelineFunction>(entity =>
        {
            entity.HasIndex(f => new { f.PipelineId });
        });
    }
}
