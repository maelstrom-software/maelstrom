const INTO_RESULT: [&str; 6] = [
    "AddArtifactResponse",
    "AddJobRequest",
    "AddLayerRequest",
    "AddLayerResponse",
    "GetContainerImageResponse",
    "GetJobStateCountsResponse",
];

fn main() {
    let mut b = tonic_build::configure();
    for resp in INTO_RESULT {
        b = b.message_attribute(resp, "#[derive(maelstrom_macro::IntoResult)]");
    }

    let enum_proto = |b: &mut tonic_build::Builder, name, remote| {
        *b = b.clone().enum_attribute(
            name,
            format!(
                "#[derive(maelstrom_macro::IntoProtoBuf, maelstrom_macro::TryFromProtoBuf)] \
                 #[proto(type_path = {remote}, reverse)]"
            ),
        );
    };
    enum_proto(&mut b, "JobDevice", "maelstrom_base::JobDevice");
    enum_proto(&mut b, "JobMountFsType", "maelstrom_base::JobMountFsType");
    enum_proto(&mut b, "ArtifactType", "maelstrom_base::ArtifactType");

    let message_proto = |b: &mut tonic_build::Builder, name, remote| {
        *b = b.clone().message_attribute(
            name,
            format!(
                "#[derive(maelstrom_macro::IntoProtoBuf, maelstrom_macro::TryFromProtoBuf)] \
                 #[proto(type_path = {remote}, reverse)]"
            ),
        );
    };
    message_proto(&mut b, "JobMount", "maelstrom_base::JobMount");
    message_proto(&mut b, "JobSpec", "maelstrom_base::JobSpec");

    b = b.message_attribute(
        "ContainerImage",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_container::ContainerImage, reverse)]",
    );
    b = b.field_attribute("ContainerImage.config", "#[proto(option)]");
    b = b.message_attribute(
        "OciImageConfiguration",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_container::ImageConfiguration, reverse)]",
    );
    b = b.field_attribute("OciImageConfiguration.architecture", "#[proto(option)]");
    b = b.field_attribute("OciImageConfiguration.os", "#[proto(option)]");
    b = b.field_attribute("OciImageConfiguration.rootfs", "#[proto(option)]");
    b = b.message_attribute(
        "OciConfig",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_container::Config, reverse)]",
    );
    b = b.message_attribute(
        "OciRootFs",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_container::RootFs, reverse)]",
    );
    b = b.enum_attribute(
        "JobOutcomeCompleted.status",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_base::JobStatus, reverse)]",
    );
    b = b.message_attribute(
        "JobEffects",
        "#[derive(maelstrom_macro::TryFromProtoBuf, maelstrom_macro::IntoProtoBuf)] \
         #[proto(type_path = maelstrom_base::JobEffects, reverse, option_all)]",
    );

    b.compile(&["src/items.proto"], &["src/"]).unwrap();
}
