## Copying bash_rc
echo "Copying bashrc file.."
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/make_life_easier.sh > ~/.bashrc
echo "Copied successfully"

## install homebrew
echo "Installing Homebrew.."
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
echo "Homebrew successfully installed"

## install git
echo "Installing git.."
brew install git
echo "git successfully installed"

## install docker and co
echo "Installing docker.."
brew cask install docker 
brew install docker-compose docker-machine xhyve docker-machine-driver-xhyve
sudo chown root:wheel $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
sudo chmod u+s $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
echo "docker successfully installed"

## install git completion
echo "Installing git completion.."
curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-completion.bash > ~/.git-completion.bash
echo "git completion successfully installed"

## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_1_7.zip > iTerm2.zip
unzip iTerm2.zip &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."
cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

## install visual studio code
echo "Installing VS Code.."
brew cask install visual-studio-code
## this might ask you for your password
code --version
echo "VS Code successfully installed"

## install tldr https://tldr.sh/
echo "Installing tldr..."
brew install tldr
echo "tldr installed. "

## install bash completion
echo "Installing bash completion.."
brew install bash-completion
echo "bash completion successfully installed"

## update terminal prompt
echo "Updating terminal prompt.."
curl https://raw.githubusercontent.com/git/git/master/contrib/completion/git-prompt.sh >> ~/.git-prompt.sh
echo "Terminal prompt successfully updated"

## create global gitignore
echo "Creating a global gitignore.."
git config --global core.excludesfile ~/.gitignore
touch ~/.gitignore
echo '.DS_Store' >> ~/.gitignore
echo '.idea' >> ~/.gitignore
echo "Global gitignore created"

## install the project
echo "Installing the analytics project.."
mkdir ~/repos/
cd ~/repos/
git clone git@gitlab.com:gitlab-data/analytics.git
echo "Analytics repo successfully installed"

## install goto
echo "Installing goto.."
brew install goto
touch ~/.inputrc
echo -e "\$include /etc/inputrc\nset colored-completion-prefix on" >> ~/.inputrc
echo "goto successfully installed.. Adding alias for analytics.."
cd ~/repos/analytics
goto -r analytics ~/repos/analytics
echo "analytics goto alias successfully added"
## you can now type "goto analytics" and you're in the right place
## gl_open is now an alias to open this on gitlab.com

## install dbt
echo "Installing dbt.."
brew update
brew tap fishtown-analytics/dbt
brew install dbt
echo "dbt successfully installed.. Printing version.."
dbt --version
echo "Setting up dbt profile.."
mkdir ~/.dbt
touch ~/.dbt/profiles.yml
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml >> ~/.dbt/profiles.yml
echo "dbt profile created.. You will need to edit this file later."
## you will need to edit this file

## install the dbt completion script
echo "Installing dbt completion script.."
curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > ~/.dbt-completion.bash
echo "dbt completion script successfully installed"

## Add refresh command
echo "alias dbt_refresh='dbt clean ; dbt deps ; dbt seed'" >> ~/.bash_profile

## install anaconda
echo "Installing anaconda.."
brew cask install anaconda
echo "export PATH=/usr/local/anaconda3/bin:"$PATH"" >> ~/.bash_profile
echo "anaconda installed succesfully"

echo "export SNOWFLAKE_TRANSFORM_WAREHOUSE=ANALYST_XS" >> ~/.bash_profile
echo "export SNOWFLAKE_LOAD_DATABASE=RAW" >> ~/.bash_profile
echo "export SNOWFLAKE_SNAPSHOT_DATABASE='SNOWFLAKE'" >> ~/.bash_profile
echo "source ~/.bashrc" >> ~/.bash_profile
echo "source ~/.bashrc"
echo "source ~/.bash_profile"

echo "Onboarding script ran successfully"
