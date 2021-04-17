use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::{Deref, DerefMut},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::Error,
    format::Format,
    lru::LruVec,
    system::UuidGenerator,
    vfs::{Vfs, VfsSyncOption},
};

const LOCK_FILENAME: &str = "grebedb_lock.lock";
const METADATA_FILENAME: &str = "grebedb_meta.grebedb";
const METADATA_NEW_FILENAME: &str = "grebedb_meta.grebedb.tmp";
const METADATA_OLD_FILENAME: &str = "grebedb_meta_prev.grebedb";
const METADATA_COPY_FILENAME: &str = "grebedb_meta_copy.grebedb";

pub type PageId = u64;
pub type RevisionId = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct Page<T> {
    pub uuid: Uuid, // should match metadata
    pub id: PageId,
    pub revision: RevisionId,
    pub deleted: bool,
    pub content: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata<M> {
    pub uuid: Uuid, // uuid for the entire database
    pub revision: RevisionId,
    pub id_counter: PageId, // current allocated ID
    pub free_id_list: Vec<PageId>,
    pub root_id: Option<PageId>,
    pub auxiliary: Option<M>,
}

struct PageCache<T> {
    lru: LruVec<PageId>,
    cached_pages: HashMap<PageId, Page<T>>,
    modified_pages: HashSet<PageId>, // pages in cache not yet written to disk
}

impl<T> PageCache<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity >= 1);

        Self {
            lru: LruVec::new(capacity),
            cached_pages: HashMap::with_capacity(capacity + 1), // +1 due to statement order
            modified_pages: HashSet::with_capacity(capacity + 1),
        }
    }

    pub fn modified_pages(&self) -> &HashSet<PageId> {
        &self.modified_pages
    }

    pub fn clear_modified_pages(&mut self) {
        self.modified_pages.clear();
    }

    pub fn contains_page(&mut self, page_id: PageId) -> bool {
        self.cached_pages.contains_key(&page_id)
    }

    pub fn get_touched(&mut self, page_id: PageId) -> Option<&Page<T>> {
        self.lru.touch(&page_id);
        self.cached_pages.get(&page_id)
    }

    pub fn _peek(&self, page_id: PageId) -> Option<&Page<T>> {
        self.cached_pages.get(&page_id)
    }

    pub fn get_touched_mut(&mut self, page_id: PageId) -> Option<&mut Page<T>> {
        self.lru.touch(&page_id);
        self.modified_pages.insert(page_id);
        self.cached_pages.get_mut(&page_id)
    }

    pub fn set_page_revision(&mut self, page_id: PageId, revision: RevisionId) {
        let mut page = self.cached_pages.get_mut(&page_id).unwrap();
        page.revision = revision;
    }

    #[must_use]
    pub fn put_touched(&mut self, page_id: PageId, page: Page<T>) -> Option<EvictedPage<T>> {
        self.cached_pages.insert(page_id, page);
        self.modified_pages.insert(page_id);

        if let Some(evicted_page_id) = self.lru.insert(page_id) {
            let modified = self.modified_pages.remove(&evicted_page_id);
            let page = self.cached_pages.remove(&evicted_page_id).unwrap();

            Some(EvictedPage {
                id: evicted_page_id,
                page,
                modified,
            })
        } else {
            None
        }
    }

    // Reserved for when borrow checker doesn't agree
    pub fn take(&mut self, page_id: PageId) -> Option<Page<T>> {
        self.cached_pages.remove(&page_id)
    }

    // Reserved for when borrow checker doesn't agree
    pub fn untake(&mut self, page_id: PageId, page: Page<T>) {
        self.cached_pages.insert(page_id, page);
    }
}

struct EvictedPage<T> {
    id: PageId,
    page: Page<T>,
    modified: bool,
}

#[derive(Default)]
struct FileTracker {
    pub pending_sync: HashSet<PageId>, // files written but not fsync()-ed
    pub pending_promotion: HashSet<PageId>, // files not renamed to the main filename
}

#[derive(Default)]
struct CounterTracker {
    dirty: bool,
    revision: RevisionId,                // revision counter in memory
    revision_on_persistence: RevisionId, // revision counter that is saved on disk
    root_id: Option<PageId>,
    id_counter: PageId, // current allocated page ID counter
    free_id_list: VecDeque<PageId>,
}

impl CounterTracker {
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn unset_dirty(&mut self) {
        self.dirty = false;
    }

    pub fn revision(&self) -> RevisionId {
        self.revision
    }

    pub fn revision_on_persistence(&self) -> RevisionId {
        self.revision_on_persistence
    }

    pub fn root_id(&self) -> Option<PageId> {
        self.root_id
    }

    pub fn set_root_id(&mut self, value: Option<PageId>) {
        self.dirty = true;
        self.root_id = value;
    }

    pub fn id_counter(&self) -> PageId {
        self.id_counter
    }

    pub fn free_id_list(&self) -> &VecDeque<PageId> {
        &self.free_id_list
    }

    pub fn restore(
        &mut self,
        revision: RevisionId,
        root_id: Option<PageId>,
        id_counter: PageId,
        free_id_list: &[PageId],
    ) {
        assert!(self.revision == 0);
        assert!(self.revision_on_persistence == 0);
        assert!(self.root_id == None);
        assert!(self.id_counter == 0);
        assert!(self.free_id_list.is_empty());

        self.revision = revision;
        self.revision_on_persistence = revision;
        self.root_id = root_id;
        self.id_counter = id_counter;
        self.free_id_list.extend(free_id_list);
    }

    pub fn new_page_id(&mut self) -> PageId {
        self.dirty = true;

        if let Some(id) = self.free_id_list.pop_front() {
            id
        } else {
            self.id_counter += 1;
            self.id_counter
        }
    }

    pub fn free_page_id(&mut self, page_id: PageId) {
        self.dirty = true;

        self.free_id_list.push_back(page_id);
    }

    pub fn increment_revision(&mut self) {
        self.dirty = true;
        self.revision += 1;
    }

    pub fn set_revision_persisted(&mut self) {
        self.revision_on_persistence = self.revision;
    }
}

enum RevisionFlag {
    Current,
    New,
    NewUnsync,
}

#[derive(Debug, Clone)]
pub struct PageTableOptions {
    pub open_mode: PageOpenMode,
    pub page_cache_size: usize,
    pub keys_per_node: usize,
    pub file_locking: bool,
    pub file_sync: VfsSyncOption,
    pub compression_level: Option<i32>,
}

impl Default for PageTableOptions {
    fn default() -> Self {
        Self {
            open_mode: PageOpenMode::default(),
            page_cache_size: 64,
            keys_per_node: 1024,
            file_locking: true,
            file_sync: VfsSyncOption::Data,
            compression_level: Some(3),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageOpenMode {
    LoadOnly,
    CreateOnly,
    LoadOrCreate,
    ReadOnly,
}

impl Default for PageOpenMode {
    fn default() -> Self {
        Self::LoadOrCreate
    }
}

pub struct PageTable<T, M = ()>
where
    T: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned + Clone,
{
    options: PageTableOptions,
    vfs: Box<dyn Vfs + Sync + Send>,
    format: Format,
    page_cache: PageCache<T>,
    file_tracker: FileTracker,
    counter_tracker: CounterTracker,
    uuid_generator: UuidGenerator,
    uuid: Uuid,
    closed: bool,
    auxiliary_metadata: Option<M>,
}

impl<T, M> PageTable<T, M>
where
    T: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned + Clone,
{
    pub fn open(
        mut vfs: Box<dyn Vfs + Sync + Send>,
        options: PageTableOptions,
    ) -> Result<Self, Error> {
        if matches!(
            options.open_mode,
            PageOpenMode::LoadOnly | PageOpenMode::ReadOnly
        ) && !Self::metadata_file_exists(vfs.as_ref())?
        {
            return Err(Error::InvalidFileFormat {
                path: "(directory contents)".to_string(),
                message: "not a database",
            });
        }

        if options.file_locking {
            vfs.lock(LOCK_FILENAME)?;
        }

        let metadata_file_exists = Self::metadata_file_exists(vfs.as_ref())?;

        let mut format = Format::default();
        format.set_compression_level(options.compression_level);

        let mut table = Self {
            options: options.clone(),
            vfs,
            format,
            page_cache: PageCache::new(options.page_cache_size),
            uuid: Uuid::nil(),
            file_tracker: FileTracker::default(),
            counter_tracker: CounterTracker::default(),
            uuid_generator: UuidGenerator::new(),
            closed: false,
            auxiliary_metadata: None,
        };

        match options.open_mode {
            PageOpenMode::LoadOnly | PageOpenMode::ReadOnly => {
                table.load_and_restore_metadata()?;
            }
            PageOpenMode::CreateOnly => {
                table.save_new_metadata()?;
            }
            PageOpenMode::LoadOrCreate => {
                if metadata_file_exists {
                    table.load_and_restore_metadata()?;
                } else {
                    table.save_new_metadata()?;
                }
            }
        }

        Ok(table)
    }

    fn metadata_file_exists(vfs: &dyn Vfs) -> Result<bool, Error> {
        Ok(vfs.exists(METADATA_FILENAME)?
            || vfs.exists(METADATA_COPY_FILENAME)?
            || vfs.exists(METADATA_OLD_FILENAME)?)
    }

    pub fn root_id(&self) -> Option<PageId> {
        self.counter_tracker.root_id()
    }

    pub fn set_root_id(&mut self, value: Option<PageId>) {
        self.counter_tracker.set_root_id(value);
    }

    pub fn new_page_id(&mut self) -> PageId {
        self.counter_tracker.new_page_id()
    }

    pub fn auxiliary_metadata(&self) -> Option<&M> {
        self.auxiliary_metadata.as_ref()
    }

    pub fn auxiliary_metadata_mut(&mut self) -> Option<&mut M> {
        self.auxiliary_metadata.as_mut()
    }

    pub fn set_auxiliary_metadata(&mut self, value: Option<M>) {
        self.auxiliary_metadata = value;
    }

    pub fn get(&mut self, page_id: PageId) -> Result<Option<&T>, Error> {
        self.check_if_closed()?;

        self.get_(page_id)
    }

    fn get_(&mut self, page_id: PageId) -> Result<Option<&T>, Error> {
        self.check_page_id_counter_consistency(page_id)?;

        if !self.page_cache.contains_page(page_id) {
            self.load_page_into_cache(page_id)?;
        }

        if let Some(page) = self.page_cache.get_touched(page_id) {
            if let Some(content) = &page.content {
                Ok(Some(content))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn put(&mut self, page_id: PageId, content: T) -> Result<(), Error> {
        self.check_if_closed()?;
        self.check_if_read_only()?;

        let result = self.put_(page_id, content);

        if result.is_err() {
            self.closed = true;
        }

        result
    }

    fn put_(&mut self, page_id: PageId, content: T) -> Result<(), Error> {
        self.check_page_id_counter_consistency(page_id)?;

        let page = Page {
            uuid: self.uuid,
            id: page_id,
            revision: self.counter_tracker.revision(),
            deleted: false,
            content: Some(content),
        };

        if let Some(evicted_page_info) = self.page_cache.put_touched(page_id, page) {
            self.maybe_save_evicted_page(evicted_page_info)?;
        }

        Ok(())
    }

    pub fn update(&mut self, page_id: PageId) -> Result<Option<PageUpdateGuard<T>>, Error> {
        self.check_if_closed()?;
        self.check_if_read_only()?;

        self.update_(page_id)
    }

    fn update_(&mut self, page_id: PageId) -> Result<Option<PageUpdateGuard<T>>, Error> {
        self.check_page_id_counter_consistency(page_id)?;

        if !self.page_cache.contains_page(page_id) {
            self.load_page_into_cache(page_id)?;
        }

        if let Some(page) = self.page_cache.get_touched_mut(page_id) {
            if page.content.is_some() {
                Ok(Some(PageUpdateGuard::new(page)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, page_id: PageId) -> Result<(), Error> {
        self.check_if_closed()?;
        self.check_if_read_only()?;

        let result = self.remove_(page_id);

        if result.is_err() {
            self.closed = true;
        }

        result
    }

    fn remove_(&mut self, page_id: PageId) -> Result<(), Error> {
        self.check_page_id_counter_consistency(page_id)?;

        let page = Page {
            uuid: self.uuid,
            id: page_id,
            revision: self.counter_tracker.revision(),
            deleted: true,
            content: None,
        };

        if let Some(evicted_page_info) = self.page_cache.put_touched(page_id, page) {
            self.maybe_save_evicted_page(evicted_page_info)?;
        }

        self.counter_tracker.free_page_id(page_id);

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        self.check_if_closed()?;
        self.check_if_read_only()?;

        let result = self.commit_();

        if result.is_err() {
            self.closed = true;
        }

        result
    }

    fn commit_(&mut self) -> Result<(), Error> {
        if !self.is_anything_modified() {
            return Ok(());
        }

        self.counter_tracker.increment_revision();

        self.save_all_modified_pages()?;
        self.sync_pending_page_files()?;
        self.file_tracker.pending_sync.clear();
        self.save_metadata()?;
        self.commit_counters();
        self.promote_page_filenames()?;
        self.file_tracker.pending_promotion.clear();
        self.page_cache.clear_modified_pages();

        Ok(())
    }

    fn is_anything_modified(&self) -> bool {
        self.counter_tracker.is_dirty() || !self.page_cache.modified_pages().is_empty()
    }

    fn load_and_restore_metadata(&mut self) -> Result<(), Error> {
        let metadata: Metadata<M> = self
            .format
            .read_file(self.vfs.as_mut(), &METADATA_FILENAME)?;

        self.uuid = metadata.uuid;

        self.counter_tracker.restore(
            metadata.revision,
            metadata.root_id,
            metadata.id_counter,
            &metadata.free_id_list,
        );

        self.auxiliary_metadata = metadata.auxiliary;

        // TODO: the copy backup file could be read if the main metadata file
        // is unreadable

        Ok(())
    }

    fn save_new_metadata(&mut self) -> Result<(), Error> {
        self.uuid = self.uuid_generator.new_uuid();

        // We check for the backup file too in case the main file disappears
        if self.vfs.exists(METADATA_FILENAME)?
            || self.vfs.exists(METADATA_COPY_FILENAME)?
            || self.vfs.exists(METADATA_OLD_FILENAME)?
        {
            return Err(Error::InvalidMetadata {
                message: "database already exists",
            });
        }

        self.save_metadata()?;

        Ok(())
    }

    fn load_page(
        &mut self,
        page_id: PageId,
        revision_flag: RevisionFlag,
    ) -> Result<Option<Page<T>>, Error> {
        let path = make_path(page_id, revision_flag);

        if !self.vfs.exists(&path)? {
            return Ok(None);
        }

        let page: Page<T> = self.format.read_file(self.vfs.as_mut(), &path)?;

        if !self.uuid.is_nil() && page.uuid != self.uuid {
            return Err(Error::InvalidPageData {
                page: page_id,
                message: "wrong UUID",
            });
        }

        if page.id != page_id {
            return Err(Error::InvalidPageData {
                page: page_id,
                message: "wrong page ID",
            });
        }

        Ok(Some(page))
    }

    fn load_latest_known_page(&mut self, page_id: PageId) -> Result<Option<Page<T>>, Error> {
        if self.file_tracker.pending_sync.contains(&page_id) {
            let page_2 = self.load_page(page_id, RevisionFlag::NewUnsync)?;

            if let Some(page) = page_2 {
                if page.revision <= self.counter_tracker.revision() {
                    return Ok(Some(page));
                }
            }
        }

        let page_1 = self.load_page(page_id, RevisionFlag::New)?;

        if let Some(page) = page_1 {
            if page.revision <= self.counter_tracker.revision() {
                self.maybe_queue_page_for_filename_promotion(&page);

                return Ok(Some(page));
            }
        }

        let page_0 = self.load_page(page_id, RevisionFlag::Current)?;

        if let Some(page) = page_0 {
            if page.revision <= self.counter_tracker.revision() {
                return Ok(Some(page));
            } else {
                return Err(Error::InvalidPageData {
                    page: page_id,
                    message: "missing page",
                });
            }
        }

        Ok(None)
    }

    fn load_page_into_cache(&mut self, page_id: PageId) -> Result<bool, Error> {
        let page = self.load_latest_known_page(page_id)?;

        if let Some(page) = page {
            if page.deleted || page.content.is_none() {
                return Ok(false);
            }

            if let Some(evicted_page_info) = self.page_cache.put_touched(page_id, page) {
                self.maybe_save_evicted_page(evicted_page_info)?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn save_page(&mut self, page_id: PageId, page: &Page<T>) -> Result<(), Error> {
        self.check_if_read_only()?;

        if self.options.file_sync == VfsSyncOption::None {
            self.save_page_by_overwrite(page_id, page)?;
        } else {
            self.save_page_with_delayed_sync(page_id, page)?;
        }
        // TODO: provide an option for the user to decide, or stop queueing once
        // the queues are getting relatively full,
        // but it will introduce more code path test complexity.
        // } else {
        // self.save_page_by_atomic(page_id, page)?;
        // }

        Ok(())
    }

    fn save_page_by_overwrite(&mut self, page_id: PageId, page: &Page<T>) -> Result<(), Error> {
        let path_1 = make_path(page_id, RevisionFlag::New);
        self.format
            .write_file(self.vfs.as_mut(), &path_1, page, VfsSyncOption::None)?;
        Ok(())
    }

    fn save_page_with_delayed_sync(
        &mut self,
        page_id: PageId,
        page: &Page<T>,
    ) -> Result<(), Error> {
        let path_2 = make_path(page_id, RevisionFlag::NewUnsync);

        self.format
            .write_file(self.vfs.as_mut(), &path_2, page, VfsSyncOption::None)?;

        self.file_tracker.pending_sync.insert(page_id);

        Ok(())
    }

    fn _save_page_by_atomic(&mut self, page_id: PageId, page: &Page<T>) -> Result<(), Error> {
        let path_1 = make_path(page_id, RevisionFlag::New);
        let path_1_temp = format!("{}.tmp", &path_1);

        self.format.write_file(
            self.vfs.as_mut(),
            &path_1_temp,
            page,
            self.options.file_sync,
        )?;

        self.vfs.rename_file(&path_1_temp, &path_1)?;
        self.file_tracker.pending_promotion.insert(page_id);

        Ok(())
    }

    fn save_page_from_cache(&mut self, page_id: PageId) -> Result<(), Error> {
        self.check_if_read_only()?;

        let page = self.page_cache.take(page_id).unwrap();
        let result = self.save_page(page_id, &page);
        self.page_cache.untake(page_id, page);

        result?;

        Ok(())
    }

    fn commit_counters(&mut self) {
        self.counter_tracker.unset_dirty();
        self.counter_tracker.set_revision_persisted();
    }

    fn save_metadata(&mut self) -> Result<(), Error> {
        self.check_if_read_only()?;

        let metadata = Metadata {
            uuid: self.uuid,
            revision: self.counter_tracker.revision(),
            id_counter: self.counter_tracker.id_counter(),
            root_id: self.counter_tracker.root_id(),
            free_id_list: self
                .counter_tracker
                .free_id_list()
                .iter()
                .cloned()
                .collect(),
            auxiliary: self.auxiliary_metadata.clone(),
        };

        if self.vfs.exists(METADATA_FILENAME)? {
            let data = self.vfs.read(METADATA_FILENAME)?;
            self.vfs
                .write(METADATA_OLD_FILENAME, &data, self.options.file_sync)?;
        }

        if self.options.file_sync == VfsSyncOption::None {
            self.format.write_file(
                self.vfs.as_mut(),
                METADATA_FILENAME,
                metadata.clone(),
                self.options.file_sync,
            )?;
        } else {
            self.format.write_file(
                self.vfs.as_mut(),
                METADATA_NEW_FILENAME,
                metadata.clone(),
                self.options.file_sync,
            )?;

            self.vfs
                .rename_file(METADATA_NEW_FILENAME, METADATA_FILENAME)?;
        }

        self.format.write_file(
            self.vfs.as_mut(),
            METADATA_COPY_FILENAME,
            metadata,
            self.options.file_sync,
        )?;

        Ok(())
    }

    fn maybe_save_evicted_page(&mut self, evicted_page_info: EvictedPage<T>) -> Result<(), Error> {
        if self.options.open_mode != PageOpenMode::ReadOnly && evicted_page_info.modified {
            self.save_evicted_page(evicted_page_info.id, evicted_page_info.page)?;
        }

        Ok(())
    }

    fn save_evicted_page(&mut self, page_id: PageId, mut page: Page<T>) -> Result<(), Error> {
        self.counter_tracker.increment_revision();
        page.revision = self.counter_tracker.revision();

        self.save_page(page_id, &page)?;

        Ok(())
    }

    fn save_all_modified_pages(&mut self) -> Result<(), Error> {
        let page_ids: Vec<PageId> = self.page_cache.modified_pages().iter().cloned().collect();

        for page_id in page_ids {
            self.page_cache
                .set_page_revision(page_id, self.counter_tracker.revision());

            self.save_page_from_cache(page_id)?;
        }

        Ok(())
    }

    fn sync_pending_page_files(&mut self) -> Result<(), Error> {
        let page_ids: Vec<PageId> = self.file_tracker.pending_sync.iter().cloned().collect();

        for page_id in page_ids {
            self.sync_pending_page_file(page_id)?;
        }

        Ok(())
    }

    fn sync_pending_page_file(&mut self, page_id: PageId) -> Result<(), Error> {
        let path_1 = make_path(page_id, RevisionFlag::New);
        let path_2 = make_path(page_id, RevisionFlag::NewUnsync);

        self.vfs.sync_file(&path_2, self.options.file_sync)?;
        self.vfs.rename_file(&path_2, &path_1)?;
        self.file_tracker.pending_promotion.insert(page_id);

        Ok(())
    }

    fn promote_page_filename(&mut self, page_id: PageId) -> Result<(), Error> {
        self.check_if_read_only()?;

        assert!(self.file_tracker.pending_sync.is_empty());

        let path_0 = make_path(page_id, RevisionFlag::Current);
        let path_1 = make_path(page_id, RevisionFlag::New);

        self.vfs.rename_file(&path_1, &path_0)?;

        Ok(())
    }

    fn maybe_queue_page_for_filename_promotion(&mut self, page: &Page<T>) {
        if self.options.open_mode != PageOpenMode::ReadOnly
            && page.revision <= self.counter_tracker.revision_on_persistence()
        {
            // This case is possible when the process crashed after
            // writing metadata, but before all filenames were promoted.
            // Possibly in the future, the queue is too large and not all pages
            // were promoted to reduce memory usage.

            self.file_tracker.pending_promotion.insert(page.id);
        }
    }

    fn promote_page_filenames(&mut self) -> Result<(), Error> {
        assert!(self.counter_tracker.revision_on_persistence() == self.counter_tracker.revision());
        assert!(self.file_tracker.pending_sync.is_empty());

        let page_ids: Vec<PageId> = self
            .file_tracker
            .pending_promotion
            .iter()
            .cloned()
            .collect();

        for page_id in page_ids {
            self.promote_page_filename(page_id)?;
        }

        Ok(())
    }

    fn check_if_closed(&self) -> Result<(), Error> {
        if self.closed {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn check_if_read_only(&self) -> Result<(), Error> {
        if let PageOpenMode::ReadOnly = &self.options.open_mode {
            Err(Error::ReadOnly)
        } else {
            Ok(())
        }
    }

    fn check_page_id_counter_consistency(&self, page_id: PageId) -> Result<(), Error> {
        if page_id <= self.counter_tracker.id_counter() {
            Ok(())
        } else {
            Err(Error::InvalidPageData {
                page: page_id,
                message: "requested page beyond id counter",
            })
        }
    }
}

impl<T, M> Drop for PageTable<T, M>
where
    T: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned + Clone,
{
    fn drop(&mut self) {
        if self.options.file_locking {
            let _ = self.vfs.unlock(LOCK_FILENAME);
        }
    }
}

pub struct PageUpdateGuard<'a, T> {
    page: &'a mut Page<T>,
    content: Option<T>,
}

impl<'a, T> PageUpdateGuard<'a, T> {
    pub fn new(page: &'a mut Page<T>) -> Self {
        let content = page.content.take().unwrap();

        Self {
            page,
            content: Some(content),
        }
    }
}

impl<'a, T> Deref for PageUpdateGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.content.as_ref().unwrap()
    }
}

impl<'a, T> DerefMut for PageUpdateGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.content.as_mut().unwrap()
    }
}

impl<'a, T> Drop for PageUpdateGuard<'a, T> {
    fn drop(&mut self) {
        let content = self.content.take().unwrap();
        self.page.content.replace(content);
    }
}

fn make_path(page_id: PageId, revision_flag: RevisionFlag) -> String {
    format!(
        "{}/{}",
        split_number(page_id),
        make_filename(page_id, revision_flag)
    )
}

fn make_filename(page_id: PageId, revision_flag: RevisionFlag) -> String {
    format!(
        "grebedb_{:016x}_{}.grebedb",
        page_id,
        match revision_flag {
            RevisionFlag::Current => 0,
            RevisionFlag::New => 1,
            RevisionFlag::NewUnsync => 2,
        }
    )
}

fn split_number(mut id: u64) -> String {
    let mut parts = [0u64; 8];
    let bits = 8;
    let mask = 0xff;

    for index in (0..bits).rev() {
        parts[index] = id & mask;
        id >>= bits;
    }

    format!(
        "{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}/{:02x}",
        parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]
    )
}

#[cfg(test)]
mod tests {
    use crate::vfs::MemoryVfs;

    use super::*;

    #[test]
    fn test_split_number() {
        assert_eq!(&split_number(0), "00/00/00/00/00/00/00");
        assert_eq!(&split_number(1), "00/00/00/00/00/00/00");
        assert_eq!(&split_number(0xaabb_ccdd), "00/00/00/00/aa/bb/cc");
        assert_eq!(&split_number(0xaabb_ccdd_1122_3344), "aa/bb/cc/dd/11/22/33");
    }

    #[test]
    fn test_make_filename() {
        assert_eq!(
            &make_filename(0, RevisionFlag::Current),
            "grebedb_0000000000000000_0.grebedb"
        );
        assert_eq!(
            &make_filename(0, RevisionFlag::New),
            "grebedb_0000000000000000_1.grebedb"
        );
        assert_eq!(
            &make_filename(12345678, RevisionFlag::Current),
            "grebedb_0000000000bc614e_0.grebedb"
        );
        assert_eq!(
            &make_filename(0xaabb_ccdd, RevisionFlag::New),
            "grebedb_00000000aabbccdd_1.grebedb"
        );
    }

    #[test]
    fn test_page_table_create_load() {
        let vfs = MemoryVfs::new();

        let options = PageTableOptions {
            open_mode: PageOpenMode::CreateOnly,
            ..Default::default()
        };

        let mut page_table = PageTable::<i32>::open(Box::new(vfs.clone()), options).unwrap();

        let page_id = page_table.new_page_id();
        page_table.put(page_id, 789).unwrap();

        page_table.commit().unwrap();

        drop(page_table);

        let options = PageTableOptions {
            open_mode: PageOpenMode::LoadOnly,
            ..Default::default()
        };

        let mut page_table = PageTable::<i32>::open(Box::new(vfs), options).unwrap();

        let content = page_table.get(page_id).unwrap();
        assert_eq!(content.cloned(), Some(789));
    }

    #[test]
    fn test_page_table_create_load_exists() {
        let vfs = MemoryVfs::new();

        let options = PageTableOptions {
            open_mode: PageOpenMode::LoadOnly,
            ..Default::default()
        };

        assert!(PageTable::<()>::open(Box::new(vfs.clone()), options).is_err());

        let options = PageTableOptions {
            open_mode: PageOpenMode::CreateOnly,
            ..Default::default()
        };

        let _page_table = PageTable::<()>::open(Box::new(vfs.clone()), options).unwrap();

        let _page_table =
            PageTable::<()>::open(Box::new(vfs), PageTableOptions::default()).unwrap();
    }

    #[test]
    fn test_page_table_get_put() {
        let vfs = MemoryVfs::new();
        let mut page_table =
            PageTable::<i32>::open(Box::new(vfs), PageTableOptions::default()).unwrap();

        let page_id = page_table.new_page_id();

        assert_eq!(page_table.get(page_id).unwrap(), None);

        page_table.put(page_id, 789).unwrap();

        let content = page_table.get(page_id).unwrap();

        assert_eq!(content.cloned(), Some(789));

        page_table.set_root_id(Some(page_id));
        assert_eq!(Some(page_id), page_table.root_id());
    }

    #[test]
    fn test_page_table_update() {
        let vfs = MemoryVfs::new();
        let mut page_table =
            PageTable::<i32>::open(Box::new(vfs.clone()), PageTableOptions::default()).unwrap();

        let page_id = page_table.new_page_id();

        page_table.put(page_id, 789).unwrap();

        {
            let mut guard = page_table.update(page_id).unwrap().unwrap();
            *guard = 123;
        }

        let content = page_table.get(page_id).unwrap();
        assert_eq!(content.cloned(), Some(123));

        page_table.commit().unwrap();

        drop(page_table);

        let mut page_table =
            PageTable::<i32>::open(Box::new(vfs), PageTableOptions::default()).unwrap();

        let content = page_table.get(page_id).unwrap();
        assert_eq!(content.cloned(), Some(123));
    }

    #[test]
    fn test_page_table_many_on_single_page() {
        let vfs = MemoryVfs::new();
        let mut page_table =
            PageTable::<i32>::open(Box::new(vfs), PageTableOptions::default()).unwrap();

        let page_id = page_table.new_page_id();

        for num in 0..10 {
            page_table.put(page_id, 1000 + num).unwrap();
        }

        let content = page_table.get(page_id).unwrap();

        assert_eq!(content.cloned(), Some(1000 + 9));
    }

    #[test]
    fn test_page_table_many_pages() {
        let vfs = MemoryVfs::new();
        let mut page_table =
            PageTable::<u64>::open(Box::new(vfs), PageTableOptions::default()).unwrap();

        let mut first_page_id = None;

        // `num` must be bigger than page cache size
        for num in 0..100 {
            let page_id = page_table.new_page_id();

            if first_page_id.is_none() {
                first_page_id = Some(page_id);
            }

            page_table.put(page_id, 1000 + num).unwrap();
        }

        for num in 0..100 {
            let content = page_table.get(first_page_id.unwrap() + num).unwrap();

            assert_eq!(content.cloned(), Some(1000 + num));
        }
    }

    #[test]
    fn test_page_table_remove() {
        let vfs = MemoryVfs::new();
        let mut page_table =
            PageTable::<i32>::open(Box::new(vfs), PageTableOptions::default()).unwrap();

        let page_id = page_table.new_page_id();
        let page_id_2 = page_table.new_page_id();

        page_table.put(page_id, 123).unwrap();
        page_table.put(page_id_2, 456).unwrap();

        page_table.remove(page_id).unwrap();

        assert!(page_table.get(page_id).unwrap().is_none());

        // removing already removed should not error
        page_table.remove(page_id).unwrap();
        assert!(page_table.get(page_id).unwrap().is_none());

        let page_id_3 = page_table.new_page_id();
        assert_eq!(page_id_3, page_id); // check that id is recycled from free list
        assert_eq!(page_table.get(page_id_3).unwrap(), None);
        assert_eq!(page_table.get(page_id_2).unwrap().cloned(), Some(456));
    }
}
