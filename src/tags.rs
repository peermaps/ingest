use osmpbf;
use vadeen_osm::Tag;

pub fn get_meta(_tags: osmpbf::TagIter, info: osmpbf::Info) -> vadeen_osm::Meta {
    let author = vadeen_osm::AuthorInformation {
        change_set: info.changeset().unwrap() as u64,
        uid: info.uid().unwrap() as u64,
        user: info.user().unwrap().unwrap().to_string(),
        created: info.milli_timestamp().unwrap(),
    };

    let mut tags = Vec::with_capacity(_tags.len());
    _tags.for_each(|t| {
        tags.push(Tag {
            key: t.0.to_string(),
            value: t.1.to_string(),
        })
    });

    return vadeen_osm::Meta {
        version: Some(info.version().unwrap() as u32),
        tags: tags,
        author: Some(author),
    };
}

pub fn get_dense_meta(
    _tags: osmpbf::DenseTagIter,
    info: osmpbf::DenseNodeInfo,
) -> vadeen_osm::Meta {
    let author = vadeen_osm::AuthorInformation {
        change_set: info.changeset() as u64,
        uid: info.uid() as u64,
        user: info.user().unwrap().to_string(),
        created: info.milli_timestamp(),
    };

    let mut tags = Vec::with_capacity(_tags.len());
    _tags.for_each(|t| {
        tags.push(Tag {
            key: t.0.to_string(),
            value: t.1.to_string(),
        })
    });
    return vadeen_osm::Meta {
        version: Some(info.version() as u32),
        tags: tags,
        author: Some(author),
    };
}
