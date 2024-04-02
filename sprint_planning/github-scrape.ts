// npx ts-node github-scrape.ts

// Import required modules
import axios from 'axios';

// GitHub API endpoint
const apiUrl = 'https://api.github.com';

// Function to fetch issues with a specific label for a given repository
async function getIssuesWithLabel(owner: string, repo: string, label: string, token: string): Promise<void> {
  try {
    let page = 1;
    let issues: any[] = [];

    // Fetch issues from GitHub API with authentication and pagination
    while (true) {
      const response = await axios.get(`${apiUrl}/repos/${owner}/${repo}/issues`, {
        params: {
          // labels: label,
          state: 'open',
          page,
          per_page: 100, // Adjust per_page based on your needs, max is 100
        },
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (response.data.length === 0) {
        // No more issues, break out of the loop
        break;
      }

      // Concatenate current page of issues to the array
      issues = issues.concat(response.data);

      // Move to the next page
      page++;
    }

    // Display issue details
    const nameOf = (l: any) => l?.name;
    issues
      .filter(i => i.state !== 'closed' && !i.pull_request)
      .forEach((issue: any) => {
        console.log(`${issue.number}|${issue.title}|${issue.assignee?.login || ''}|${JSON.stringify(issue.labels?.map(nameOf))}|https://github.com/medic/cht-user-management/issues/${issue.number}`);
      });
  } catch (error: any) {
    console.error('Error fetching issues:', error.message);
  }
}

// Replace these values with your GitHub repository details and personal access token
const owner = 'medic';
const repo = 'cht-user-management';
const label = 'your-label-name';
const accessToken = '';

// Call the function with the provided repository details and personal access token
getIssuesWithLabel(owner, repo, label, accessToken);