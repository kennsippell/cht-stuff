const month = new Date().getMonth();
const start = new Date(`${month}-01-2024`).getTime();
const end = new Date(`${month+1}-01-2024`).getTime();

const emissions = (await CHTCore.DB.get().get('_local/rulesStateStore')).rulesStateStore.targetState.targets['count-households-visited'].emissions;
const details = Object.keys(emissions)
  .map(key => {
    const reportKeys = Object.keys(emissions[key]);
    const visit = reportKeys.find(secondKey => {
      const emission = emissions[key][secondKey];
      return emission.pass && emission.date >= start && emission.date < end;
    });

    if (!visit) { return false; }

    return { 
      contact: key.substring('undefined-'.length, 46), 
      date: emissions[key][visit].date,
    };
  })
  .filter(Boolean);


const contacts = (await CHTCore.DB.get().allDocs({ keys: details.map(d => d.contact), include_docs: true }))
  .rows.map(row => row.doc).filter(Boolean);

const sorted = details.sort((a,b) => a.date - b.date);
sorted
  .map(detail => {
    const contact = contacts.find(doc => doc?._id === detail.contact);
    return `Visited ${contact?.name} ${new Date(detail.date).toDateString()}`;
  });
